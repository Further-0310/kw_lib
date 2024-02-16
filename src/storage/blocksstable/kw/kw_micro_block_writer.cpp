#include "kw_micro_block_writer.h"
// #include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
KwMicroBlockWriter::KwMicroBlockWriter()
    : micro_block_size_limit_(0),
      column_count_(0),
      rowkey_column_count_(0),
      data_buffer_("MicrBlocWriter"),
      index_buffer_("MicrBlocWriter"),
      col_desc_array_(nullptr),
      need_calc_column_chksum_(false),
      is_inited_(false)
{}

KwMicroBlockWriter::~KwMicroBlockWriter() {}

int KwMicroBlockWriter::init(const int64_t micro_block_size_limit,
      const int64_t rowkey_column_count,
      const int64_t column_count,
      const common::ObIArray<ObColDesc> *col_desc_array/* = nullptr*/,
      const bool need_calc_column_chksum/* = false*/)
{
    int ret = 0;
    // reset();
    //check_input_param
    if(micro_block_size_limit <= 0 || rowkey_column_count <= 0 
        || column_count <= 0 || column_count < rowkey_column_count){
        return -1;
    }

    micro_block_size_limit_ = micro_block_size_limit;
    rowkey_column_count_ = rowkey_column_count;
    column_count_ = column_count;
    need_calc_column_chksum_ = need_calc_column_chksum;
    need_check_lob_ = false;
    if(NULL != (col_desc_array_ = col_desc_array)){
        for(int64_t i = 0; !need_check_lob_ && i < col_desc_array_->count(); ++i){
            need_check_lob_ = col_desc_array_->at(i).col_type_.is_lob_storage();
        }
    }
    is_inited_ = true;
    return ret;
}

int KwMicroBlockWriter::reserve_header(
      const int64_t column_count,
      const int64_t rowkey_column_count,
      const bool need_calc_column_chksum)
{
    int ret = 0;//OB_SUCCESS
    if (column_count < 0) { ret = -1; } // OB_INVALID_ARGUMENT, column_count of sparse row is 0 // warn "column_count was invalid"
    else {
        const int32_t header_size = KwMicroBlockHeader::get_serialize_size(column_count, need_calc_column_chksum);
        header_ = reinterpret_cast<KwMicroBlockHeader*>(data_buffer_.data());

        if(0 > data_buffer_.advance(header_size)){ ret = -2; } // warn "data buffer fail to advance header size."
        else {
            memset(header_, 0, header_size);
            header_->magic_ = 1005LL; // MICRO_BLOCK_HEADER_MAGIC;
            header_->version_ = 2LL; // MICRO_BLOCK_HEADER_VERSION;
            header_->header_size_ = header_size;
            header_->column_count_ = static_cast<int32_t>(column_count);
            header_->rowkey_column_count_ = static_cast<int32_t>(rowkey_column_count);
            header_->row_store_type_ = FLAT_ROW_STORE;
            header_->has_column_checksum_ = need_calc_column_chksum;
            if(header_->has_column_checksum_){
                header_->column_checksums_ = reinterpret_cast<int64_t *>(data_buffer_.data() + 64);
                // ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET = 64;
            }
            else { header_->column_checksums_ = nullptr; }
        }
    }
    return ret;
}

int KwMicroBlockWriter::inner_init()
{
    int ret = 0;//OB_SUCCESS;
    if(!is_inited_){ ret = -1; } // warn "not init"
    else if(data_buffer_.is_dirty()){} // has been inner_inited, do nothing
    else if (0 > data_buffer_.ensure_space(DEFAULT_DATA_BUFFER_SIZE)) { ret = -2; } // warn "data buffer fail to ensure space."
    else if (0 > index_buffer_.ensure_space(DEFAULT_INDEX_BUFFER_SIZE)) { ret = -3; } // warn "index buffer fail to ensure space."
    //以上ensure_space涉及到data_buffer的alloc，有点复杂，未实现share下的MTL相关
    else if (0 > reserve_header(column_count_, rowkey_column_count_, need_calc_column_chksum_)) { ret = -4; } // warn "micro block writer fail to reserve header."
    else if (0 > index_buffer_.write(static_cast<int32_t>(0))) { ret = -5; } // warn "micro block writer fail to reserve header."
    else if (data_buffer_.length() != get_data_base_offset() || index_buffer_.length() != get_index_base_offset()){ ret = -6; }
    return ret;
}

int KwMicroBlockWriter::process_out_row_columns(const KwDatumRow &row)
{
    int ret = 0;
    if(!need_check_lob_){}
    else if(nullptr == col_desc_array_ || row.get_column_count() != col_desc_array_->count()) { return -1; } // OB_ERR_UNEXPECTED, warn "unexpected column count not match"
    else if(!has_lob_out_row_){
        for(int64_t i = 0; !has_lob_out_row_ && 0 == ret && i < row.get_column_count(); ++i){
            // KwDatum &datum = row.datums_[i];
            KwStorageDatum &datum = row.storage_datums_[i];
            if(col_desc_array_->at(i).col_type_.is_lob_storage()){
                if (datum.is_nop() ||datum.is_null()) {}
                else if (datum.len_ < sizeof(ObLobCommon)) { ret = -2; } // warn "Unexpected lob datum len"
                else {
                    const ObLobCommon &lob_common = datum.get_lob_data();
                    has_lob_out_row_ = !lob_common.in_row_;
                    STORAGE_LOG(DEBUG, "chaser debug lob out row", K(has_lob_out_row_), K(lob_common), K(datum));
                }
            }
        }
    }
    // 官方注释
    // uncomment this after varchar overflow supported
    //} else if (need_check_string_out) {
    //  if (!has_string_out_row_ && row.storage_datums_[i].is_outrow()) {
    //    has_string_out_row_ = true;
    //   }
    //}
    return ret;
}

int KwMicroBlockWriter::append_row(const KwDatumRow &row)
{
    int ret = 0;
    int64_t pos = 0;
    if(!is_inited_){ ret = -1; }
    else if(!row.is_valid()){ ret = -19; }
    else if(0 > (ret = inner_init())) {} // warn "failed to inner init"
    else if(0 > (ret = process_out_row_columns(row))) {} // warn "Failed to process out row columns"
    else{
        if(row.get_column_count() != header_->column_count_){ ret = -2; } // warn "append row column count is not consistent with init column count"
        else if (0 > (ret = row_writer_.write(rowkey_column_count_, row, data_buffer_.current(), data_buffer_.remain(), pos))) { // 行数据写入函数
            // ret = -13; // 若为buffer空间不足，warn "row writer fail to write row."
        }
        // is_exceed_limit(pos) 不额外写函数
        else if (header_->row_count_ > 0 && get_future_block_size(pos) > micro_block_size_limit_) { ret = -1; } // buffer空间不足，debug "micro block exceed limit"
        
        // try_to_append_row(pos) 不额外写函数
        else if (get_future_block_size(pos) > block_size_upper_bound_) { ret = -3; } //OB_BUF_NOT_ENOUGH, buffer空间不足
        
        else{ // finish_row(pos) 不额外写函数
            if (pos <= 0) { ret = -15; } // warn "length was invalid"
            else if (0 > data_buffer_.advance(pos)) { ret = -16; } // warn "data buffer fail to advance."
            else {
                int32_t row_offset = static_cast<int32_t>(data_buffer_.length() - header_->header_size_);
                if(0 > index_buffer_.write(row_offset)) { ret = -17; } // warn "index buffer fail to write row offset."
                else { ++ header_->row_count_; }
            }
        }
        if(0 == ret){
            if(header_->has_column_checksum_ && 0 > cal_column_checksum(row, header_->column_checksums_)){ ret = -18; } // warn "fail to cal column chksum"
            else {
                cal_row_stat(row);
                if(KW_MICRO_BLOCK_MERGE_VERIFY_LEVEL::KW_NONE < micro_block_merge_verify_level_){ // need_cal_row_checksum()
                    micro_block_checksum_ = cal_row_checksum(row, micro_block_checksum_);
                }
            }
        }
    }
    return ret;
}


int KwMicroBlockWriter::build_block(char *&buf, int64_t &size)
{
    int ret = 0;//OB_SUCCESS;
    if(!is_inited_ || !data_buffer_.is_dirty()){ ret = -1; } // OB_NOT_INIT, warn "should init writer before append row" or "unexpected empty block"
    else{
        if (last_rows_count_ == header_->row_count_){ header_->single_version_rows_ = 1; }
        header_->row_index_offset_ = static_cast<int32_t>(data_buffer_.length());
        header_->contain_uncommitted_rows_ = contain_uncommitted_row_;
        header_->max_merged_trans_version_ = max_merged_trans_version_;
        header_->has_string_out_row_ = has_string_out_row_;
        header_->all_lob_in_row_ = !has_lob_out_row_;
        header_->is_last_row_last_flag_ = is_last_row_last_flag_;
        if (data_buffer_.remain() < get_index_size()) { ret = -2; } // OB_SIZE_OVERFLOW, warn "row data buffer is overflow."
        else if(0 > data_buffer_.write(index_buffer_.data(), get_index_size())){ ret = -3; } // warn "data buffer fail to write index."
        else{
            buf = data_buffer_.data();
            size = data_buffer_.length();
        }
    }
    return ret;
}

void KwMicroBlockWriter::reset()
{
  KwIMicroBlockWriter::reuse();
  micro_block_size_limit_ = 0;
  column_count_ = 0;
  rowkey_column_count_ = 0;
  need_calc_column_chksum_ = false;
  row_writer_.reset();
  header_ = nullptr;
  data_buffer_.reset();
  index_buffer_.reset();
  col_desc_array_ = nullptr;
  is_inited_ = false;
}

void KwMicroBlockWriter::reuse()
{
  KwIMicroBlockWriter::reuse();
  row_writer_.reset();
  data_buffer_.reuse();
  index_buffer_.reuse();
  header_ = nullptr;
}

} // namespace blocksstable
    
} // namespace oceanbase
