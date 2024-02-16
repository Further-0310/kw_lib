#define USING_LOG_PREFIX STORAGE

#include "kw_micro_block_encoder.h"
// #include "kw_icolumn_encoder.h"

namespace oceanbase
{
using namespace storage;
namespace blocksstable 
{
using namespace common;

template<typename T>
T *KwMicroBlockEncoder::alloc_encoder()
{
    T *encoder = NULL;
    int ret = 0;
    if(0 > (ret = encoder_allocator_.alloc(encoder))) {}
    return encoder;
}

void KwMicroBlockEncoder::free_encoder(ObIColumnEncoder *encoder)
{
    if(NULL != encoder) {
        // encoder_allocator_.free(encoder);
        encoder = NULL;
    }
}

template <typename T>
int KwMicroBlockEncoder::try_encoder(ObIColumnEncoder *&encoder, const int64_t column_index)
{
    int ret = 0;
    encoder = NULL;
    bool suitable = false;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(column_index < 0 || column_index > ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(ctx_.encoder_opt_.enable<T>()) {
        T *e = alloc_encoder<T>();
        if(NULL == e) { ret = -3; } // OB_ALLOCATE_MEMORY_FAILED
        else {
            col_ctxs_.at(column_index).try_set_need_sort(e->get_type(), column_index);
            if(0 > (ret = e->init(col_ctxs_.at(column_index), column_index, datum_rows_))) {}
            else if(0 > (ret = e->traverse(suitable))) {}
        }
        if(NULL != e && (0 > ret || !suitable)){
            free_encoder(e);
            e = NULL;
        }
        else {
            encoder = e;
        }
    }
    return ret;
}

KwMicroBlockEncoder::KwMicroBlockEncoder() : ctx_(), header_(NULL),
    data_buffer_("EncodeDataBuf"/*blocksstable::OB_ENCODING_LABEL_DATA_BUFFER*/),
    datum_rows_(), all_col_datums_(),
    buffered_rows_checksum_(0), estimate_size_(0), estimate_size_limit_(0),
    header_size_(0), expand_pct_(DEFAULT_ESTIMATE_REAL_SIZE_PCT),
    row_buf_holder_("EncodeRowBuffer"/*blocksstable::OB_ENCODING_LABEL_ROW_BUFFER*/, OB_MALLOC_MIDDLE_BLOCK_SIZE),
    encoder_allocator_(encoder_sizes, common::ObModIds::OB_ENCODER_ALLOCATOR),
    string_col_cnt_(0), estimate_base_store_size_(0), length_(0),
    is_inited_(false)
{
}

KwMicroBlockEncoder::~KwMicroBlockEncoder() { reset(); }

int64_t KwMicroBlockEncoder::get_data_size() const {
  int64_t data_size = data_buffer_.length();
  if (data_size == 0 && is_inited_) { //lazy allocate
    data_size = KwMicroBlockHeader::get_serialize_size(
        ctx_.column_cnt_, ctx_.need_calc_column_chksum_);
  }
  return data_size;
}

int KwMicroBlockEncoder::init(const ObMicroBlockEncodingCtx &ctx)
{
    // can be init twice
    int ret = 0;
    if(is_inited_) {
        reuse();
        is_inited_ = false;
    }

    int64_t col_cnt = ctx.column_cnt_;

    if(!ctx.is_valid()) { ret = -1; } // OB_INVALID_ARGUMENT
    else if(0 > (ret = encoders_.reserve(col_cnt))) {}
    else if(0 > (ret = encoder_allocator_.init())) {}
    else if(0 > (ret = hashtables_.reserve(col_cnt))) {}
    else if(0 > (ret = multi_prefix_trees_.reserve(col_cnt))) {}
    else if(0 > (ret = col_ctxs_.reserve(col_cnt))) {}
    else if(0 > (ret = init_all_col_values(ctx))) {}
    else if(0 > (ret = row_buf_holder_.init(ctx.macro_block_size_, MTL_ID()))) {}
    else {
        // TODO bin.lb: shrink all_col_values_ size
        estimate_base_store_size_ = 0;
        for (int64_t i = 0; i < col_cnt; ++i) {
            estimate_base_store_size_ += get_estimate_base_store_size_map()[ctx.col_descs_->at(i).col_type_.get_type()];
            if(!need_check_lob_ && ctx.col_descs_->at(i).col_type_.is_lob_storage()) {
                need_check_lob_ = true;
            }
        }
        ctx_ = ctx;
        is_inited_ = true;
    }
    return ret;
}

int KwMicroBlockEncoder::inner_init()
{
    int ret = 0;
    if(!is_inited_) { ret = -1; } // OB_NOT_INIT
    else if(data_buffer_.is_dirty()) {} // has been inner_inited, do nothing
    else if(!ctx_.is_valid()) { ret = -2; } // OB_ERR_UNEXPECTED
    else if(0 > (ret = data_buffer_.ensure_space(DEFAULT_DATA_BUFFER_SIZE))) {}
    else if(0 > (ret = reserve_header(ctx_))) {}
    else {
        update_estimate_size_limit(ctx_);
    }
    return ret;
}

void KwMicroBlockEncoder::reset()
{
    KwIMicroBlockWriter::reuse();
    is_inited_ = false;
    //ctx_
    header_ = nullptr;
    data_buffer_.reset();
    datum_rows_.reset();
    FOREACH(cv, all_col_datums_) {
        ObColDatums *p = *cv;
        OB_DELETE(ObColDatums, blocksstable::OB_ENCODING_LABEL_PIVOT, p);
    }
    all_col_datums_.reset();
    estimate_size_ = 0;
    estimate_size_limit_ = 0;
    header_size_ = 0;
    expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT;
    estimate_base_store_size_ = 0;
    row_buf_holder_.reset();
    fix_data_encoders_.reset();
    var_data_encoders_.reset();
    free_encoders();
    encoders_.reset();
    hashtables_.reset();
    multi_prefix_trees_.reset();
    row_indexs_.reset();
    deep_copy_indexes_.reset();
    col_ctxs_.reset();
    string_col_cnt_ = 0;
    length_ = 0;
}

void KwMicroBlockEncoder::reuse()
{
    KwIMicroBlockWriter::reuse();
    // is_inited_
    // ctx_
    data_buffer_.reuse();
    header_ = nullptr;
    datum_rows_.reuse();
    FOREACH(c, all_col_datums_) {
        (*c)->reuse();
    }
    buffered_rows_checksum_ = 0;
    estimate_size_ = 0;
    // estimate_size_limit_
    // header_size_
    // expand_pct_
    // estimate_base_store_size_ should only recalculate with initialization
    // row_buf_holder_ no need to reuse
    fix_data_encoders_.reuse();
    var_data_encoders_.reuse();
    free_encoders();
    encoders_.reuse();
    hashtables_.reuse();
    multi_prefix_trees_.reuse();
    row_indexs_.reuse();
    col_ctxs_.reuse();
    string_col_cnt_ = 0;
    length_ = 0;
}

int KwMicroBlockEncoder::calc_and_validate_checksum(const KwDatumRow &row)
{
    int ret = 0;
    micro_block_checksum_ = cal_row_checksum(row, micro_block_checksum_);
    if(micro_block_merge_verify_level_ < 3) {} // do nothing
    //MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE
    else if(datum_rows_.count() <= 0) { ret = -1; } // OB_ERR_UNEXPECTED
    else {
        ObConstDatumRow &buf_row = datum_rows_.at(datum_rows_.count() - 1);
        for(int64_t col_idx = 0; col_idx < buf_row.get_column_count(); ++col_idx) {
            buffered_rows_checksum_ = buf_row.datums_[col_idx].checksum(buffered_rows_checksum_);
        }
        if(buffered_rows_checksum_ != micro_block_checksum_) { ret = -2; } // OB_CHECKSUM_ERROR
    }
    return ret;
}

void KwMicroBlockEncoder::dump_diagnose_info() const {}

int KwMicroBlockEncoder::init_all_col_values(const ObMicroBlockEncodingCtx &ctx)
{
    int ret = 0;
    if(0 > (ret = all_col_datums_.reserve(ctx.column_cnt_))) {}
    for (int64_t i = all_col_datums_.count(); i < ctx.column_cnt_ && 0 == ret; ++i) {
        ObColDatums *c = OB_NEW(ObColDatums, blocksstable::OB_ENCODING_LABEL_PIVOT);
        if(NULL == c) { ret = -1; } // OB_ALLOCATE_MEMORY_FAILED
        else if(0 > (ret = all_col_datums_.push_back(c))) {
            OB_DELETE(ObColDatums, blocksstable::OB_ENCODING_LABEL_PIVOT, c);
        }
    }
    return ret;
}

void KwMicroBlockEncoder::print_micro_block_encoder_status() const {}

void KwMicroBlockEncoder::update_estimate_size_limit(const ObMicroBlockEncodingCtx &ctx)
{
    expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT; //150L
    if(ctx.real_block_size_ > 0) {
        const int64_t prev_expand_pct = ctx.estimate_block_size_ * 100 / ctx.real_block_size_;
        expand_pct_ = prev_expand_pct != 0 ? prev_expand_pct : 1;
    }

    // We should make sure expand_pct_ never equal to 0
    if(0 == expand_pct_) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "expand_pct equal to zero",
            K_(expand_pct), K(ctx.estimate_block_size_), K(ctx.real_block_size_));
    }

    header_size_ = 
        KwMicroBlockHeader::get_serialize_size(ctx.column_cnt_, ctx.need_calc_column_chksum_)
        + (sizeof(ObColumnHeader)) * ctx.column_cnt_;
    int64_t data_size_limit = (2 * ctx.micro_block_size_ - header_size_) > 0 ?
        (2 * ctx.micro_block_size_ - header_size_) : (2 * ctx.micro_block_size_);
    //TODO  use 4.1.0.0 for version judgment
    if(ctx.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
        data_size_limit = MAX(data_size_limit, DEFAULT_MICRO_MAX_SIZE/*256K*/);
    }
    estimate_size_limit_ = std::min(data_size_limit * expand_pct_ / 100, ctx.macro_block_size_);
    // LOG_TRACE
}

int KwMicroBlockEncoder::try_to_append_row(const int64_t &store_size)
{
    int ret = 0;
    // header_size_ = micro_header_size + column_header_size
    if(store_size + estimate_size_ + header_size_ > block_size_upper_bound_) {
        ret = -1; // OB_BUF_NOT_ENOUGH
    }
    return ret;
}

int KwMicroBlockEncoder::append_row(const KwDatumRow &row)
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(!row.is_valid()) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(row.get_column_count() != ctx_.column_cnt_) { ret = -3; } // OB_INVALID_ARGUMENT
    else if(0 > (ret = inner_init())) {}
    else {
        if(datum_rows_.empty()) {
            // Allocate a block to hold all rows in one micro block, size is 3*estimate_size_limit_.
            // The maximum size of a micro block is estimate_size_limit_, but set 3 times of it in memory
            // due to memory struct need to store some other meta information.
            // When micro block is too small, estimate_size_ could be 0. Each row bceomes a large row.
            if(0 != estimate_size_limit_ && 0 > (ret = row_buf_holder_.try_alloc(3 * estimate_size_limit_))) {
                ret = -4; // OB_ALLOCATE_MEMORY_FAILED
            }
            else if(0 > (ret = init_column_ctxs())) {}
        }
    }

    if(0 == ret) {
        int64_t store_size = 0;
        ObConstDatumRow datum_row;
        if(datum_rows_.count() >= MAX_MICRO_BLOCK_ROW_CNT) {
            ret = -5; // OB_BUF_NOT_ENOUGH
        }
        else if(0 > (ret = process_out_row_columns(row))) {}
        else if(0 > (ret = copy_and_append_row(row, store_size))) {
            // if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            //     LOG_WARN("copy and append row failed", K(ret));
            // }
        }
        else if(header_->has_column_checksum_ && 0 > (ret = cal_column_checksum(row, header_->column_checksums_))) {}
        else if(need_cal_row_checksum() && 0 > (ret = calc_and_validate_checksum(row))) {}
        else {
            cal_row_stat(row);
            estimate_size_ += store_size;
        }
    }
    return ret;
}

int KwMicroBlockEncoder::pivot()
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(datum_rows_.empty()) { ret = -2; } // OB_INNER_STAT_ERROR
    else {
        for(int64_t i = 0; 0 == ret && i < ctx_.column_cnt_; ++i) {
            ObColDatums &c = *all_col_datums_.at(i);
            if(0 > (ret = c.resize(datum_rows_.count()))) {}
            else {
                int64_t pos = 0;
                for(; pos + 8 <= datum_rows_.count(); pos += 8) {
                    c.at(pos + 0) = datum_rows_.at(pos + 0).get_datum(i);
                    c.at(pos + 1) = datum_rows_.at(pos + 1).get_datum(i);
                    c.at(pos + 2) = datum_rows_.at(pos + 2).get_datum(i);
                    c.at(pos + 3) = datum_rows_.at(pos + 3).get_datum(i);
                    c.at(pos + 4) = datum_rows_.at(pos + 4).get_datum(i);
                    c.at(pos + 5) = datum_rows_.at(pos + 5).get_datum(i);
                    c.at(pos + 6) = datum_rows_.at(pos + 6).get_datum(i);
                    c.at(pos + 7) = datum_rows_.at(pos + 7).get_datum(i);
                }
                for(; pos < datum_rows_.count(); ++pos) {
                    c.at(pos) = datum_rows_.at(pos).get_datum(i);
                }
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::reserve_header(const ObMicroBlockEncodingCtx &ctx)
{
    int ret = 0;
    if(ctx.column_cnt_ < 0) { ret = -1; } // OB_INVALID_ARGUMENT
    else {
        int32_t header_size = KwMicroBlockHeader::get_serialize_size(ctx.column_cnt_, ctx.need_calc_column_chksum_);
        header_ = reinterpret_cast<KwMicroBlockHeader*>(data_buffer_.data());
        if(0 > (ret = data_buffer_.advance(header_size))) {}
        else {
            memset(header_, 0, header_size);
            header_->magic_ = 1005L; // MICRO_BLOCK_HEADER_MAGIC
            header_->version_ = 2L; // MICRO_BLOCK_HEADER_VERSION
            header_->header_size_ = header_size;
            header_->row_store_type_ = ctx.row_store_type_;
            header_->column_count_ = ctx.column_cnt_;
            header_->rowkey_column_count_ = ctx.rowkey_column_cnt_;
            header_->has_column_checksum_ = ctx.need_calc_column_chksum_;
            if(header_->has_column_checksum_) {
                header_->column_checksums_ = reinterpret_cast<int64_t *>(data_buffer_.data() + KwMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET);
            }
            else {
                header_->column_checksums_ = nullptr;
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::store_encoding_meta_and_fix_cols(int64_t &encoding_meta_offset) 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else {
        // detect extend value bit
        header_->extend_value_bit_ = 0;
        FOREACH(e, encoders_) {
            ObIColumnEncoder::EncoderDesc &desc = (*e)->get_desc();
            if (desc.has_null_ || desc.has_nope_) {
                header_->extend_value_bit_ = 1;
                if (desc.has_nope_) {
                    header_->extend_value_bit_ = 2;
                    break;
                }
            }
        }
        FOREACH(e, encoders_) { ((*e))->set_extend_value_bit(header_->extend_value_bit_); }

        const int64_t header_size = data_buffer_.pos();
        const int64_t col_header_size = ctx_.column_cnt_ * (sizeof(ObColumnHeader));
        encoding_meta_offset = header_size + col_header_size;

        if(0 > (ret = data_buffer_.advance_zero(col_header_size))) {}
        else {
            for(int64_t i = 0; 0 == ret && i < encoders_.count(); ++i) {
                int64_t pos_bak = data_buffer_.pos();
                ObIColumnEncoder::EncoderDesc &desc = encoders_.at(i)->get_desc();
                if(0 > (ret = encoders_.at(i)->store_meta(data_buffer_))) {}
                else { //更新ColumnHeader参数信息
                    ObColumnHeader &ch = encoders_.at(i)->get_column_header();
                    if(data_buffer_.pos() > pos_bak) {
                        ch.offset_ = static_cast<uint32_t>(pos_bak - encoding_meta_offset);
                        ch.length_ = static_cast<uint32_t>(data_buffer_.pos() - pos_bak);
                    }
                    else if(ObColumnHeader::RAW == encoders_.at(i)->get_type()) {
                        // column header offset records the start pos of the fix data, if needed
                        ch.offset_ = static_cast<uint32_t>(pos_bak - encoding_meta_offset);
                    }
                    ch.obj_type_ = static_cast<uint8_t>(encoders_.at(i)->get_obj_type());
                }
                header_->row_data_offset_ = static_cast<int32_t>(data_buffer_.pos());
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::build_block(char *&buf, int64_t &size)
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(datum_rows_.empty()) { ret = -2; } // OB_INNER_STAT_ERROR
    else if(0 > (ret = pivot())) {}
    else if(0 > (ret = row_indexs_.reserve(datum_rows_.count()))) {}
    else if(0 > (ret = encoder_detection())) {}
    else {
        // STORAGE_LOG DEBUG

        // <1> store encoding metas and fix cols data
        int64_t encoding_meta_offset = 0;
        if(0 > (ret = store_encoding_meta_and_fix_cols(encoding_meta_offset))) {}

        // <2> set row data store offset
        int64_t fix_data_size = 0;
        if(0 == ret) {
            if(0 > (ret = set_row_data_pos(fix_data_size))) {}
            else { header_->var_column_count_ = static_cast<uint16_t>(var_data_encoders_.count()); }
        }

        // <3> fill row data (i.e. vat data)
        if(0 == ret) {
            if(0 > (ret = fill_row_data(fix_data_size))) {}
        }

        // <4> fill row index
        if(0 == ret) {
            if(var_data_encoders_.empty()) {
                header_->row_index_byte_ = 0;
            }
            else {
                header_->row_index_byte_ = 2;
                if(row_indexs_.at(row_indexs_.count() - 1) > UINT16_MAX) {
                    header_->row_index_byte_ = 4;
                }
                ObIntegerArrayGenerator gen;
                const int64_t row_index_size = row_indexs_.count() * header_->row_index_byte_;
                if(0 > (ret = gen.init(data_buffer_.current(), header_->row_index_byte_))) {}
                else if(0 > (ret = data_buffer_.advance_zero(row_index_size))) {}
                else {
                    for (int64_t idx = 0; idx < row_indexs_.count(); ++idx) {
                        gen.get_array().set(idx, row_indexs_.at(idx));
                    }
                }
            }
        }

        // <5> fill header
        if(0 == ret) {
            header_->row_count_ = static_cast<uint32_t>(datum_rows_.count());
            header_->has_string_out_row_ = has_string_out_row_;
            header_->all_lob_in_row_ = !has_lob_out_row_;

            const int64_t header_size = header_->header_size_;
            char *data = data_buffer_.data() + header_size;
            FOREACH(e, encoders_) {
                memcpy(data, &(*e)->get_column_header(), sizeof(ObColumnHeader));
                data += sizeof(ObColumnHeader);
            }
        }

        if(0 == ret) {
            // update encoding context
            ctx_.estimate_block_size_ += estimate_size_;
            ctx_.real_block_size_ += data_buffer_.length() - encoding_meta_offset;
            ctx_.micro_block_cnt_++;
            ObPreviousEncoding pe;
            for (int64_t idx = 0; 0 == ret && idx < encoders_.count(); ++idx) {
                ObIColumnEncoder *e = encoders_.at(idx);
                pe.type_ = static_cast<ObColumnHeader::Type>(e->get_column_header().type_);
                if(ObColumnHeader::is_inter_column_encoder(pe.type_)) {
                    pe.ref_col_idx_ = static_cast<ObSpanColumnEncoder *>(e)->get_ref_col_idx();
                }
                else {
                    pe.ref_col_idx_ = 0;
                }
                if(ObColumnHeader::STRING_PREFIX == pe.type_) {
                    pe.last_prefix_length_ = col_ctxs_.at(idx).last_prefix_length_;
                }
                if(idx < ctx_.previous_encodings_.count()) {
                    if(0 > (ret = ctx_.previous_encodings_.at(idx).put(pe))) {}
                    //if (ctx_->previous_encodings_.at(idx).last_1 != pe.type_) {
                        //LOG_DEBUG("encoder changing", K(idx),
                            //"previous type", ctx_->previous_encodings_.at(idx).last_,
                            //"current type", pe);
                    //}
                }
                else {
                    ObPreviousEncodingArray<2/*ObMicroBlockEncodingCtx::MAX_PREV_ENCODING_COUNT*/> pe_array;
                    if(0 > (ret = pe_array.put(pe))) {}
                    else if(0 > (ret = ctx_.previous_encodings_.push_back(pe_array))) {}
                }
            }
        }
        else {
            // Print status of current encoders for debugging
            print_micro_block_encoder_status();
            if (OB_BUF_NOT_ENOUGH == ret) {
                // buffer not enough when building a block with pivoted rows, probably caused by estimate
                // maximum block size after encoding failed, rewrite errno with OB_ENCODING_EST_SIZE_OVERFLOW
                // to force compaction task retry with flat row store type.
                ret = OB_ENCODING_EST_SIZE_OVERFLOW;
                LOG_WARN("build block failed by probably estimated maximum encoding data size overflow", K(ret));
            }
        }

        if(0 == ret) {
            buf = data_buffer_.data();
            size = data_buffer_.pos();
        }
    }
    return ret;
}

int KwMicroBlockEncoder::set_row_data_pos(int64_t &fix_data_size) 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(datum_rows_.empty()) { ret = -2; } // OB_INNER_STAT_ERROR
    else {
        // detect extend value bit size and dispatch encoders.
        int64_t ext_bit_size = 0;
        FOREACH_X(e, encoders_, 0 == ret) {
            ObIColumnEncoder::EncoderDesc &desc = (*e)->get_desc();
            if(desc.need_data_store_ && desc.is_var_data_) {
                if(0 > (ret = var_data_encoders_.push_back(*e))) {}
                else if(desc.need_extend_value_bit_store_) {
                    (*e)->get_column_header().extend_value_index_ = static_cast<int16_t>(ext_bit_size);
                    ext_bit_size += header_->extend_value_bit_;
                }
            }
        }
        fix_data_size = (ext_bit_size + CHAR_BIT - 1) / CHAR_BIT;

        // set var data position
        for (int64_t i = 0; 0 == ret && i < var_data_encoders_.count(); ++i) {
            var_data_encoders_.at(i)->get_desc().row_offset_ = fix_data_size;
            var_data_encoders_.at(i)->get_desc().row_length_ = i;
            if (0 > (ret = var_data_encoders_.at(i)->set_data_pos(fix_data_size, i))) {}
        }
        if(0 == ret && !var_data_encoders_.empty()) {
            ObColumnHeader &ch = var_data_encoders_.at(var_data_encoders_.count() - 1)->get_column_header();
            ch.attr_ |= ObColumnHeader::LAST_VAR_FIELD;
        }
    }
    return ret;
} 

inline int KwMicroBlockEncoder::store_data(ObIColumnEncoder &e,
    const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) 
{
    return e.store_data(row_id, bs, buf, len);
}

int KwMicroBlockEncoder::fill_row_data(const int64_t fix_data_size) 
{
    int ret = 0;
    ObArray<int64_t> var_lengths;//变长列偏移量
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else if(datum_rows_.empty()) { ret = -2; } // OB_INNER_STAT_ERROR
    else if(0 > (ret = var_lengths.reserve(var_data_encoders_.count()))) {}
    else if(0 > (ret = row_indexs_.push_back(0))) {}
    else {
        for(int64_t i = 0; 0 == ret && i < var_data_encoders_.count(); ++i) {
            if(0 > (ret = var_lengths.push_back(0))) {}
        }
        for(int64_t row_id = 0; 0 == ret && row_id < datum_rows_.count(); ++row_id) {
            int64_t var_data_size = 0;
            int64_t column_index_byte = 0;
            int64_t row_size = 0;
            // detect column index byte
            if (var_data_encoders_.count() > 0) {
                for(int64_t i = 0; 0 == ret && i < var_data_encoders_.count(); ++i) {
                    int64_t length = 0;
                    if(0 > (ret = var_data_encoders_.at(i)->get_var_length(row_id, length))) {}
                    else {
                        var_lengths.at(i) = length;
                        if(i > 0 && i == var_data_encoders_.count() - 1) {
                            column_index_byte = var_data_size <= UINT8_MAX ? 1 : 2;
                            if(var_data_size > UINT16_MAX) { column_index_byte = 4; }
                        }
                        var_data_size += length;
                    }
                }
            }

            // fill var data
            row_size = fix_data_size + var_data_size + (column_index_byte > 0 ? 1 : 0)
                + column_index_byte * (var_data_encoders_.count() - 1);
            char *data = data_buffer_.current();
            ObBitStream bs;
            if (0 > ret) {} 
            else if (0 > (ret = data_buffer_.advance_zero(row_size))) {} 
            else if (0 > (ret = bs.init(reinterpret_cast<unsigned char *>(data), fix_data_size))) {} 
            else {
                memset(data, 0, row_size);
                char *column_index_byte_ptr = data + fix_data_size;

                ObIntegerArrayGenerator gen;
                if (0 == ret && column_index_byte > 0) {
                    if (0 > (ret = gen.init(data + fix_data_size + 1, column_index_byte))) {}
                }
                if (0 == ret && !var_data_encoders_.empty()) {
                    char *var_data = column_index_byte_ptr;
                    if (column_index_byte > 0) {
                        *column_index_byte_ptr = static_cast<int8_t>(column_index_byte);
                        var_data +=  1 + column_index_byte * (var_data_encoders_.count() - 1);
                    }
                    int64_t offset = 0;
                    for (int64_t i = 0; OB_SUCC(ret) && i < var_data_encoders_.count(); ++i) {
                        if (i > 0) {
                            gen.get_array().set(i - 1, offset);
                        }
                        const int64_t length = var_lengths.at(i);
                        if (0 > (ret = store_data(*var_data_encoders_.at(i),
                            row_id, bs, var_data + offset, length))) {}
                        offset += length;
                    }
                }
            }
            if (0 == ret) {
                if (0 > (ret = row_indexs_.push_back(data_buffer_.pos() - header_->row_data_offset_))) {}
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::init_column_ctxs()
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT
    else {
        ObColumnEncodingCtx cc;
        for(int64_t i = 0; 0 == ret && i < ctx_.column_cnt_; ++i) {
            cc.reset();
            cc.encoding_ctx_ = &ctx_;
            if(0 > (ret = col_ctxs_.push_back(cc))) {}
        }
    }
    return ret;
}

int KwMicroBlockEncoder::process_out_row_columns(const KwDatumRow &row)
{
    // make sure in&out row status of all values in a column are same
    int ret = 0;
    if(!need_check_lob_) {}
    else if(row.get_column_count() != col_ctxs_.count()) { ret = -1; } // OB_ERR_UNEXPECTED
    else if(!has_lob_out_row_) {
        for(int64_t i = 0; !has_lob_out_row_ && 0 == ret && i < row.get_column_count(); ++i) {
            KwStorageDatum &datum = row.storage_datums_[i];
            if(datum.is_nop() || datum.is_null()) {}
            else if(datum.len_ < sizeof(ObLobCommon)) { ret = -2; } // OB_ERR_UNEXPECTED
            else {
                const ObLobCommon &lob_common = datum.get_lob_data();
                has_lob_out_row_ = !lob_common.in_row_;
                // LOG_DEBUG
            }
        }
    }
    // uncomment this after varchar overflow supported
    //} else if (need_check_string_out) {
    //  if (!has_string_out_row_ && row.storage_datums_[i].is_outrow()) {
    //    has_string_out_row_ = true;
    //   }
    //}
    return ret;
}

int KwMicroBlockEncoder::copy_and_append_row(const KwDatumRow &src, int64_t &store_size)
{
    int ret = 0;
    // performance critical, do not double check parameters in private method
    const int64_t datums_len = sizeof(ObDatum) * src.get_column_count();
    bool is_large_row = false;
    ObDatum *datum_arr = nullptr;
    if(datum_rows_.count() > 0 && (length_ + datums_len >= estimate_size_limit_ || estimate_size_ >= estimate_size_limit_)) {
        ret = -1; // OB_BUF_NOT_ENOUGH
    }
    else if(0 == datum_rows_.count() && length_ + datums_len >= estimate_size_limit_) {
        is_large_row = true;
    }
    else {
        char *datum_arr_buf = row_buf_holder_.get_buf() + length_;
        memset(datum_arr_buf, 0, datums_len);
        datum_arr = reinterpret_cast<ObDatum *>(datum_arr_buf);
        length_ += datums_len;

        store_size = 0;
        for(int64_t col_idx = 0; 0 == ret && col_idx < src.get_column_count() && !is_large_row; ++ col_idx) {
            if(0 > (ret = copy_cell(ctx_.col_descs_->at(col_idx), src.storage_datums_[col_idx], 
                                    datum_arr[col_idx], store_size, is_large_row))) {
                if (OB_BUF_NOT_ENOUGH != ret) {
                    LOG_WARN("fail to copy cell", K(ret), K(col_idx), K(src), K(store_size), K(is_large_row));
                }
            }
        }
    }

    if(0 > ret) {}
    else if(is_large_row && 0 > (ret = process_large_row(src, datum_arr, store_size))) {}
    else if(0 > (ret = try_to_append_row(store_size))) {
        // if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        //     LOG_WARN("fail to try append row", K(ret));
        // }
    }
    else {
        ObConstDatumRow datum_row(datum_arr, src.get_column_count());
        if(0 > (ret = datum_rows_.push_back(datum_row))) {}
    }
    return ret;
}

int KwMicroBlockEncoder::copy_cell(
    const ObColDesc &col_desc,
    const KwStorageDatum &src,
    ObDatum &dest,
    int64_t &store_size,
    bool &is_large_row)
{
    // For IntSC and UIntSC, normalize to 8 byte
    int ret = 0;//OB_SUCCESS;
    ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
    const bool is_int_sc = store_class == ObIntSC || store_class == ObUIntSC;
    int64_t datum_size = 0;
    dest.ptr_ = row_buf_holder_.get_buf() + length_;
    dest.pack_ = src.pack_;
    if (src.is_null()) {
        dest.set_null();
        datum_size = sizeof(uint64_t);
    } else if (src.is_nop()) {
        datum_size = dest.len_;
    } else if (OB_UNLIKELY(src.is_ext())) {
        ret = -1;//OB_NOT_SUPPORTED;
        // LOG_WARN("unsupported store extend datum type", K(ret), K(src));
    } else {
        datum_size = is_int_sc ? sizeof(uint64_t) : dest.len_;
        if (ctx_.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
            if (is_var_length_type(store_class)) {
                // For var-length type column, we need to add extra 8 bytes for estimate safety
                // e.g: ref size for dictionary, meta data for encoding etc.
                datum_size += sizeof(uint64_t);
            }
        }
    }

    if (/*OB_FAIL*/0 > ret) {}
    else if (FALSE_IT(store_size += datum_size)) {}
    else if (datum_rows_.count() > 0 && estimate_size_ + store_size >= estimate_size_limit_) {
        ret = OB_BUF_NOT_ENOUGH;
        // for large row whose size larger than a micro block default size,
        // we still use micro block to store it, but its size is unknown, need special treat
    }
    else if (datum_rows_.count() == 0 && estimate_size_ + store_size >= estimate_size_limit_) {
        is_large_row = true;
    }
    else {
        if (is_int_sc) {
            memset(const_cast<char *>(dest.ptr_), 0, datum_size);
        }
        memcpy(const_cast<char *>(dest.ptr_), src.ptr_, dest.len_);
        length_ += datum_size;
    }
    return ret;
}

int KwMicroBlockEncoder::process_large_row(
    const KwDatumRow &src,
    ObDatum *&datum_arr,
    int64_t &store_size)
{
    int ret = 0;//OB_SUCCESS;
    // copy_size is the size in memory, including the ObDatum struct and extra space for data.
    // store_size represents for the serialized data size on disk,
    const int64_t datums_len = sizeof(ObDatum) * src.get_column_count();
    int64_t copy_size = datums_len;
    int64_t var_len_column_cnt = 0;
    for (int64_t col_idx = 0; col_idx < src.get_column_count(); ++col_idx) {
        const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
        ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
        KwStorageDatum &datum = src.storage_datums_[col_idx];
        if (!datum.is_null()) {
            if (store_class == ObIntSC || store_class == ObUIntSC) {
                copy_size += sizeof(uint64_t);
            }
            else {
                copy_size += datum.len_;
                if (is_var_length_type(store_class)) {
                    ++var_len_column_cnt;
                }
            }
        }
        else {
            copy_size += sizeof(uint64_t);
        }
    }
    if (ctx_.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
        store_size = copy_size - datums_len + var_len_column_cnt * sizeof(uint64_t);
    }
    else {
        store_size = copy_size - datums_len;
    }
    if (/*OB_FAIL*/0 > (ret = row_buf_holder_.try_alloc(copy_size))) {
        // LOG_WARN("Fail to alloc large row buffer", K(ret), K(copy_size));
    } else {
        char *buf = row_buf_holder_.get_buf();
        memset(buf, 0, datums_len);
        datum_arr = reinterpret_cast<ObDatum *>(buf);
        buf += datums_len;

        for (int64_t col_idx = 0; col_idx < src.get_column_count(); ++col_idx) {
            const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
            ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
            const bool is_int_sc = (store_class == ObIntSC || store_class == ObUIntSC);
            KwStorageDatum &src_datum = src.storage_datums_[col_idx];
            ObDatum &dest_datum = datum_arr[col_idx];
            if (src_datum.is_null()) {
                dest_datum.set_null();
                MEMSET(buf, 0, sizeof(uint64_t));
                buf += sizeof(uint64_t);
            } else {
                dest_datum.ptr_ = buf;
                dest_datum.pack_ = src_datum.pack_;
                const int64_t datum_data_size = is_int_sc ? sizeof(uint64_t) : src_datum.len_;
                if (is_int_sc) {
                    memset(const_cast<char *>(dest_datum.ptr_), 0, datum_data_size);
                }
                memcpy(const_cast<char *>(dest_datum.ptr_), src_datum.ptr_, src_datum.len_);
                buf += datum_data_size;
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::prescan(const int64_t column_index) 
{
    int ret = 0;
    if(IS_NOT_INIT) {
        ret = -1; //OB_NOT_INIT;
    }
    else {
        const ObColDesc &col_desc = ctx_.col_descs_->at(column_index);
        const ObObjMeta column_type = col_desc.col_type_;
        const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(column_type.get_type())];
        int64_t type_store_size = get_type_size_map()[column_type.get_type()];
        ObColumnEncodingCtx &col_ctx = col_ctxs_.at(column_index);
        col_ctx.col_datums_ = all_col_datums_.at(column_index);

        //hash索引、前缀树相关
        // build hashtable
        ObEncodingHashTable *ht = nullptr;
        ObEncodingHashTableBuilder *builder = nullptr;
        ObMultiPrefixTree *prefix_tree = nullptr;
        //next power of 2 
        uint64_t bucket_num = datum_rows_.count() * 2;
        if(0 != (bucket_num & (bucket_num - 1))) {
            while (0 != (bucket_num & (bucket_num - 1))) {
                bucket_num = bucket_num & (bucket_num - 1);
            }
            bucket_num = bucket_num * 2;
        }
        //找到不小于bucket_num的最小2的幂次方
        const int64_t node_num = datum_rows_.count();
        if(0 > (ret = hashtable_factory_.create(bucket_num, node_num, ht))) {
            LOG_WARN("create hashtable failed", K(ret), K(bucket_num), K(node_num));
        } else if(0 > (ret = multi_prefix_tree_factory_.create(node_num, bucket_num, prefix_tree))) {
            LOG_WARN("failed to create multi-prefix tree", K(ret), K(bucket_num));
        } else {
            builder = static_cast<ObEncodingHashTableBuilder *>(ht);
            if(0 > (ret = builder->build(*col_ctx.col_datums_, col_desc))) {
                LOG_WARN("build hash table failed", K(ret), K(column_index), K(column_type));
            }
        }

        if(0 == ret) {
            col_ctx.prefix_tree_ = prefix_tree;
            if(0 > (ret = build_column_encoding_ctx(ht, store_class, type_store_size, col_ctx))) {
                LOG_WARN("build_column_encoding_ctx failed", K(ret), KP(ht), K(store_class), K(type_store_size));
            } else if(0 > (ret = hashtables_.push_back(ht))) {
                LOG_WARN("failed to push back", K(ret));
            } else if(0 > (ret = multi_prefix_trees_.push_back(prefix_tree))) {
                LOG_WARN("failed to push back multi-prefix tree", K(ret));
            }
            LOG_DEBUG("hash table", K(column_index), K(*ht));
        }

        if(0 > ret) {
            // avoid overwirte ret
            int tmp_ret = 0;
            if(0 != (tmp_ret = hashtable_factory_.recycle(ht))) {
                LOG_WARN("recycle hashtable failed", K(tmp_ret));
            }
            if(0 != (tmp_ret = multi_prefix_tree_factory_.recycle(prefix_tree))) {
                LOG_WARN("failed to recycle multi-prefix tree", K(tmp_ret));
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::encoder_detection() 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // OB_NOT_INIT;
    else {
        for(int64_t i = 0; 0 == ret && i < ctx_.column_cnt_; ++i) {
            if(0 > (ret = prescan(i))) {} // LOG_WARN
        }
        if(0 == ret) {
            int64_t extend_value_bit = 1;
            FOREACH(c, col_ctxs_) {//只要有一列有nope值，扩展值位数就设为2
                if(c->nope_cnt_ > 0) {
                    extend_value_bit = 2;
                    break;
                }
            }
            FOREACH(c, col_ctxs_) {//更新所有col_ctxs_的扩展值位数
                c->extend_value_bit_ = extend_value_bit;
            }
        }
        for(int64_t i = 0; 0 == ret && i < ctx_.column_cnt_; ++i) {
            if(0 > (ret = fast_encoder_detect(i, col_ctxs_.at(i)))) {} // LOG_WARN
            else {
                if(encoders_.count() <= i) { //fast_detect没有添加相应的encoder，但为什么不是fail？
                    if(col_ctxs_.at(i).only_raw_encoding_) { //若这列只允许原始编码？
                        ret = -2;//OB_ERR_UNEXPECTED
                        //LOG_WARN
                    } 
                    else if(0 > (ret = choose_encoder(i, col_ctxs_.at(i)))) { //手动选择encoder
                        //LOG_WARN
                    } 
                }
            }
        }
        if(0 > ret) { // 若上述失败，释放encoders_空间
            free_encoders();
        }
    }
    return ret;
}

int KwMicroBlockEncoder::fast_encoder_detect(const int64_t column_idx, const ObColumnEncodingCtx &cc) 
{
    int ret = 0;
    ObIColumnEncoder *e = NULL;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_idx < 0 || column_idx >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(nullptr != ctx_.column_encodings_  //ctx_.column_encodings_预设了不同类型的列对应的编码方法
            && ctx_.column_encodings_[column_idx] > 0 
            && ctx_.column_encodings_[column_idx] < ObColumnHeader::Type::MAX_TYPE) {
        LOG_INFO("specified encoding", K(column_idx), K(ctx_.column_encodings_[column_idx]));
        if(0 > (ret = try_encoder(e, column_idx, static_cast<ObColumnHeader::Type>(ctx_.column_encodings_[column_idx]), 0, -1))) {} //LOG_WARN
        else if(NULL == e) { ret = -3; } //OB_ERR_UNEXPECTED    预设encoder失败
    }
    else if(cc.only_raw_encoding_) { //若此列只适合原始编码，强制使用原始编码
        if(0 > (ret = force_raw_encoding(column_idx, true, e))) {} //LOG_WARN
        else if(NULL == e) { ret = -4; } //OB_ERR_UNEXPECTED
    }
    else /*if(cc.ht_->distinct_cnt() <= 1)*/ {//若此列的hash表中只有一个不重复值，采用常数编码
        if(0 > (ret = try_encoder<ObConstEncoder>(e, column_idx))) {} //LOG_WARN
    }

    if(0 == ret && NULL != e){//若成功赋值encoder，将其加入全局的encoders_数组中
        //LOG_DEBUG
        if(0 > (ret = encoders_.push_back(e))) {
            //LOG_WARN
            free_encoder(e);
            e = NULL;
        }
    }
    return ret;
}

int KwMicroBlockEncoder::try_previous_encoder(ObIColumnEncoder *&e,
    const int64_t column_index, const ObPreviousEncoding &previous) 
{ //尝试用之前的编码类型、前缀、参考列
    return try_encoder(e, column_index, previous.type_,
                previous.last_prefix_length_, previous.ref_col_idx_);
}

int KwMicroBlockEncoder::try_encoder(ObIColumnEncoder *&e, const int64_t col_idx,
      const ObColumnHeader::Type type, const int64_t last_prefix_length,
      const int64_t ref_col_idx) 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(col_idx < 0 || col_idx >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else {
        switch (type) {
            case ObColumnHeader::RAW:{
                ret = try_encoder<ObRawEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::DICT:{
                ret = try_encoder<ObDictEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::RLE:{
                ret = try_encoder<ObRLEEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::CONST:{
                ret = try_encoder<ObConstEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::INTEGER_BASE_DIFF:{
                ret = try_encoder<ObIntegerBaseDiffEncoder>(e, col_idx);
                break;
            } 
            case ObColumnHeader::STRING_DIFF:{
                ret = try_encoder<ObStringDiffEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::HEX_PACKING:{
                ret = try_encoder<ObHexStringEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::STRING_PREFIX:{
                ret = try_encoder<ObStringPrefixEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::COLUMN_EQUAL:{
                ret = ref_col_idx >= 0
                      ? try_span_column_encoder<ObColumnEqualEncoder>(e, col_idx, ref_col_idx)
                      : try_span_column_encoder<ObColumnEqualEncoder>(e, col_idx);
                break;
            }
            case ObColumnHeader::COLUMN_SUBSTR:{
                ret = ref_col_idx >= 0
                      ? try_span_column_encoder<ObInterColSubStrEncoder>(e, col_idx, ref_col_idx)
                      : try_span_column_encoder<ObInterColSubStrEncoder>(e, col_idx);
                break;
            }   
            default:{
                ret = -3;//OB_ERR_UNEXPECTED;
                break;
            }
        }
        if(0 > ret) {} //LOG_WARN
    }
    return ret;
}

int KwMicroBlockEncoder::try_previous_encoder(ObIColumnEncoder *&choose,
    const int64_t column_index, const int64_t acceptable_size, bool &try_more) 
{
    int ret = 0;
    ObIColumnEncoder *e = NULL;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_index < 0 || column_index >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else {
        bool need_calc = false; // 暂时不知道需要计算什么
        int64_t cycle_cnt = 0;
        if(32 < ctx_.micro_block_cnt_) { cycle_cnt = 16; }
        else if(16 < ctx_.micro_block_cnt_) { cycle_cnt = 8; }
        else { cycle_cnt = 4; }
        need_calc = (0 == ctx_.micro_block_cnt_ % cycle_cnt);

        if(column_index < ctx_.previous_encodings_.count()) {
            int64_t pos = ctx_.previous_encodings_.at(column_index).last_pos_;
            ObPreviousEncoding *prev = NULL;
            for(int64_t i = 0; 0 == ret && i < ctx_.previous_encodings_.at(column_index).size_; ++i) {
                //遍历此列对应的previous_encodings_二维数组中的previous_encodings_数组，一开始都是未检测过的encoder
                prev = &ctx_.previous_encodings_.at(column_index).prev_encodings_[pos];
                if(!col_ctxs_.at(column_index).detected_encoders_[prev->type_]){
                    col_ctxs_.at(column_index).detected_encoders_[prev->type_] = true;
                    if(0 > (ret = try_previous_encoder(e, column_index, *prev))) {}
                    else if(NULL != e) {
                        if(e->calc_size() <= choose->calc_size()) {
                            free_encoder(choose);
                            choose = e;
                            if(!need_calc || choose->calc_size() <= acceptable_size) {
                                try_more = false;
                                //LOG_DEBUG
                            }
                        }
                    }
                }
                pos = (pos == ctx_.previous_encodings_.at(column_index).size_ - 1) ? 0 : pos + 1;
            }
        }
    }
    return ret;
}

template <typename T>
int KwMicroBlockEncoder::try_span_column_encoder(ObIColumnEncoder *&e,
    const int64_t column_index) //需要寻找参考列的列间编码
{
    int ret = 0;
    e = NULL;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_index < 0 || column_index >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(ctx_.encoder_opt_.enable<T>() && !col_ctxs_.at(column_index).is_refed_) { //若当前列没有被参考
        bool suitable = false;
        T *encoder = alloc_encoder<T>();
        if(NULL == encoder) {ret = -3; } // OB_ALLOCATE_MEMORY_FAILED
        else {
            for(int64_t i = 0; 0 == ret && i < ctx_.column_cnt_ && !suitable; ++i) {//寻找一列参考
                if (column_index == i) {} //不能参考自己，跳过
                else if(i < column_index && ObColumnHeader::is_inter_column_encoder(encoders_.at(i)->get_type())) {} //若是当前列前面的列间编码列，由于已经参考其他列了，跳过
                else if(ctx_.col_descs_->at(i).col_type_ == ctx_.col_descs_->at(column_index).col_type_) { //若找到了与当前列类型一致的列
                    encoder->reuse(); //尝试将此列作为参考列
                    if(0 > (ret = try_span_column_encoder(encoder, column_index, i, suitable))) {}
                    else if(suitable) { //若成功，即找到了适合作为参考的列i，设列i为被参考
                        col_ctxs_.at(i).is_refed_ = true;
                        //LOG_DEBUG
                    }
                }
            }
        }
        if(0 == ret && suitable) { e = encoder; }
        else if(NULL != encoder) {
            free_encoder(encoder);
            encoder = NULL;
        }
    }
    return ret;
}

template <typename T>
int KwMicroBlockEncoder::try_span_column_encoder(ObIColumnEncoder *&e,
    const int64_t column_index, const int64_t ref_column_index)
{
    int ret = 0;
    e = NULL;
    bool suitable = false;
    if(ctx_.encoder_opt_.enable<T>() && !col_ctxs_.at(column_index).is_refed_) {
        if(ref_column_index < column_index && ObColumnHeader::is_inter_column_encoder(encoders_[ref_column_index]->get_type())) {}
        else { //若参考列在当前列之前且参考列属于列间编码，跳过不予赋值encoder
            T *encoder = alloc_encoder<T>();
            if(NULL == encoder) { ret = -3; }//OB_ALLOCATE_MEMORY_FAILED
            else if(0 > (ret =  try_span_column_encoder(encoder, column_index, ref_column_index, suitable))) {}

            if(0 == ret && suitable) { e = encoder; }
            else if(NULL != encoder) {
                free_encoder(encoder);
                encoder = NULL;
            }
        }
    }
    return ret;
}

int KwMicroBlockEncoder::try_span_column_encoder(ObSpanColumnEncoder *encoder,
    const int64_t column_index, const int64_t ref_column_index, bool &suitable)
{
    int ret = 0;
    suitable = false;
    if(nullptr == encoder || column_index < 0 || ref_column_index < 0 || ref_column_index > ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(ref_column_index > UINT16_MAX) {} //not suitable
    else if(FALSE_IT(col_ctxs_.at(column_index).try_set_need_sort(encoder->get_type(), column_index))) {}
    else if(0 > (ret = encoder->init(col_ctxs_[column_index], column_index, datum_rows_))) {}
    else if(0 > (ret = encoder->set_ref_col_idx(ref_column_index, col_ctxs_[ref_column_index]))) {}
    else if(0 > (ret = encoder->traverse(suitable))) {}
    return ret;
}

int KwMicroBlockEncoder::choose_encoder(const int64_t column_idx, ObColumnEncodingCtx &cc) 
{
    int ret = 0;
    ObIColumnEncoder *e = NULL;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_idx < 0 || column_idx >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else if(0 > (ret = try_encoder<ObRawEncoder>(e, column_idx))) {} //0.先选原始编码
    else if(NULL == e) { ret = -2; } // OB_ERR_UNEXPECTED
    else {
        bool try_more = true;
        ObIColumnEncoder *choose = e; //记录之前的encoder原始编码
        int64_t acceptable_size = choose->calc_size() / 4; //有些编码只要小于原始编码空间的1/4就不用继续找了
        if(0 > (ret = try_encoder<ObDictEncoder>(e, column_idx))) {} //1.尝试字典编码
        else if(NULL != e) {
            if(e->calc_size() < choose->calc_size()) {//若当前字典编码计算空间小于之前的选择，替换为当前字典编码
                free_encoder(choose);
                choose = e;
            }
            else {//否则释放掉字典编码
                free_encoder(e);
                e = NULL;
            }
        }

        //2.尝试之前的微块编码
        // try previous micro block encoding
        if(0 == ret && try_more) {
            if(0 > (ret = try_previous_encoder(choose, column_idx, acceptable_size, try_more))) {}
        }

        //3.若此列没有被参考，尝试列间等值编码
        // try column equal
        if(0 == ret && try_more && !cc.is_refed_ /* not refed by other */) {
            if(cc.detected_encoders_[ObColumnEqualEncoder::type_]) {}
            else if(0 > (ret = try_span_column_encoder<ObColumnEqualEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                if(e->calc_size() < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e; 
                    if(choose->calc_size() <= acceptable_size) { try_more = false; } //若当前编码计算空间小于可接受的大小，就无需再尝试了
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }

        //4.若此列没有被参考，且可以进行字符串编码，尝试列间子串编码
        // try inter column substring
        const ObObjTypeClass tc = ob_obj_type_class(ctx_.col_descs_->at(column_idx).col_type_.get_type());
        const ObObjTypeStoreClass sc = get_store_class_map()[tc];
        if(0 == ret && try_more && !cc.is_refed_) {
            if(is_string_encoding_valid(sc)){
                if(cc.detected_encoders_[ObInterColSubStrEncoder::type_]) {}
                else if(0 > (ret = try_span_column_encoder<ObInterColSubStrEncoder>(e, column_idx))) {}
                else if(NULL != e) {
                    if(e->calc_size() < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                        free_encoder(choose);
                        choose = e; 
                        if(choose->calc_size() <= acceptable_size) { try_more = false; } //若当前编码计算空间小于可接受的大小，就无需再尝试了
                    }
                    else {//否则释放掉
                        free_encoder(e);
                        e = NULL;
                    }
                }
            }
        }

        //5.若此列的独特值个数不超过行数的一半，尝试游程编码和常数编码
        // try rle and const
        if(0 == ret && try_more /*&& cc.ht_->distinct_cnt() <= datum_rows_.count() / 2*/) {
            if(cc.detected_encoders_[ObRLEEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObRLEEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                if(e->calc_size() < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e; 
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
            //先尝试RLE游程编码，成功后再看常数编码能否继续降低空间
            if(0 > ret) {} 
            else if(cc.detected_encoders_[ObConstEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObConstEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                if(e->calc_size() < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e; 
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }
        //若当前编码计算空间小于可接受的大小，就无需再尝试了
        if(0 == ret && try_more && choose->calc_size() <= acceptable_size) { try_more = false; }

        //6.若存储类型是整数类，尝试整数基数差编码
        if(0 == ret && try_more && (ObIntSC == sc || ObUIntSC == sc)) {
            if(cc.detected_encoders_[ObIntegerBaseDiffEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObIntegerBaseDiffEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                int64_t size = e->calc_size();
                if(size < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e;
                    try_more = size <= acceptable_size;//？若当前编码计算空间小于可接受的大小，需要继续尝试？
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }

        //7.若此列允许字符串编码且有定长部分数据，尝试字符串差编码
        bool string_diff_suitable = false;
        if(0 == ret && try_more && is_string_encoding_valid(sc) && cc.fix_data_size_ > 0) {
            if(cc.detected_encoders_[ObStringDiffEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObStringDiffEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                int64_t size = e->calc_size();
                if(size < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e;
                    try_more = size <= acceptable_size;//？若当前编码计算空间小于可接受的大小，需要继续尝试？
                    string_diff_suitable = true; //标记适合字符串差编码
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }

        //8.若此列允许字符串编码，也可以尝试字符串前缀编码
        bool string_prefix_suitable = false;
        if(0 == ret && try_more && is_string_encoding_valid(sc)) {
            if(cc.detected_encoders_[ObStringPrefixEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObStringPrefixEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                int64_t size = e->calc_size();
                if(size < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e;
                    try_more = size <= acceptable_size;//？若当前编码计算空间小于可接受的大小，需要继续尝试？
                    string_prefix_suitable = true; //标记适合字符串前缀编码
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }

        //9.若此列允许字符串编码且7、8两种字符串编码都不适合，可以尝试Hex串编码
        if(0 == ret && try_more && is_string_encoding_valid(sc) 
            && !string_diff_suitable && !string_prefix_suitable) {
            if(cc.detected_encoders_[ObHexStringEncoder::type_]) {}
            else if(0 > (ret = try_encoder<ObHexStringEncoder>(e, column_idx))) {}
            else if(NULL != e) {
                int64_t size = e->calc_size();
                if(size < choose->calc_size()) {//若当前编码计算空间小于之前的选择，替换之
                    free_encoder(choose);
                    choose = e;
                    try_more = size <= acceptable_size;//？若当前编码计算空间小于可接受的大小，需要继续尝试？
                }
                else {//否则释放掉
                    free_encoder(e);
                    e = NULL;
                }
            }
        }

        //10.若上述成功选择了encoder，若为列间编码类型，需要设置参考列相关参数；
        //  然后把选择的encoder加入全局encoders_数组中
        if(0 == ret) {
            //LOG_DEBUG
            if(ObColumnHeader::is_inter_column_encoder(choose->get_type())) {
                const int64_t ref_col_idx = static_cast<ObSpanColumnEncoder *>(choose)->get_ref_col_idx();
                col_ctxs_.at(ref_col_idx).is_refed_ = true;
                //LOG_DEBUG
            }
            if(0 > (ret = encoders_.push_back(choose))) {}
        }

        if(0 > ret && NULL != choose) {
            free_encoder(choose);
            choose = NULL;
        }
    }
    return ret;
}

void KwMicroBlockEncoder::free_encoders() 
{
    int ret = 0;
    FOREACH(e, encoders_) { free_encoder(*e); }
    FOREACH(ht, hashtables_) {// should continue even fail
        if(0 > (ret = hashtable_factory_.recycle(*ht))) {} // LOG_WARN
    }
    FOREACH(pt, multi_prefix_trees_) {// should continue even fail
        if(0 > (ret = multi_prefix_tree_factory_.recycle(*pt))) {} // LOG_WARN
    }
    encoders_.reuse();
    hashtables_.reuse();
    multi_prefix_trees_.reuse();
}

int KwMicroBlockEncoder::force_raw_encoding(const int64_t column_idx) 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_idx < 0 || column_idx >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else {
        const bool force_var_store = true;
        if(NULL != encoders_[column_idx]) {
            free_encoder(encoders_[column_idx]);
            encoders_[column_idx] = NULL;
        }

        ObIColumnEncoder *e = NULL;
        if(0 > (ret = force_raw_encoding(column_idx, force_var_store, e))) {}
        else if(NULL == e) { ret = -3; } // OB_ERR_UNEXPECTED
        else { encoders_[column_idx] = e; }
    }
    return ret;
}

int KwMicroBlockEncoder::force_raw_encoding(const int64_t column_idx,
    const bool force_var_store, ObIColumnEncoder *&encoder) 
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } //OB_BOT_INIT
    else if(column_idx < 0 || column_idx >= ctx_.column_cnt_) { ret = -2; } // OB_INVALID_ARGUMENT
    else {
        bool suitable = false;
        ObRawEncoder *e = alloc_encoder<ObRawEncoder>();
        if(NULL == e) { ret = -3; } // OB_ALLOCATE_MEMORY_FAILED
        else if(0 > (ret = e->init(col_ctxs_.at(column_idx), column_idx, datum_rows_))) {}
        else if(0 > (ret = e->traverse(force_var_store, suitable))) {}

        if(0 == ret) {
            if(!suitable) { ret = -4; } // OB_ERR_UNEXPECTED
            else { e->set_extend_value_bit(header_->extend_value_bit_); }
        }

        if (NULL != e && 0 > ret) {
            // free_encoder(e);
            e = NULL;
        } 
        else { encoder = e; }
    }
    return ret;
}

} // namespace blocksstable
} // namespace oceanbase
