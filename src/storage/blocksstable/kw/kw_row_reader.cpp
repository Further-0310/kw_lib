#define USING_LOG_PREFIX COMMON
#include "kw_row_reader.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
#define SET_ROW_BASIC_INFO(row) \
  { \
    row.row_flag_ = row_header_->get_row_flag(); \
    row.mvcc_row_flag_.flag_ = row_header_->get_mvcc_row_flag(); \
    row.trans_id_ = row_header_->get_trans_id(); \
  }

static uint64_t get_offset_0(const void *offset_array, const int64_t idx)
{ UNUSEDx(offset_array, idx); return INT64_MAX; }
static uint64_t get_offset_8(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint8_t*>(offset_array)[idx]; }
static uint64_t get_offset_16(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint16_t*>(offset_array)[idx]; }
static uint64_t get_offset_32(const void *offset_array, const int64_t idx)
{ return reinterpret_cast<const uint32_t*>(offset_array)[idx]; }

uint64_t (*kw_get_offset_func[KwColClusterInfoMask::BYTES_MAX])(const void *, const int64_t)
    = {get_offset_0, get_offset_8, get_offset_16, get_offset_32};

/********************KwClusterColumnReader***********************/
int KwClusterColumnReader::init(
    const char *cluster_buf,
    const uint64_t cluster_len,
    const uint64_t cluster_col_cnt,
    const KwColClusterInfoMask &info_mask)
{
  int ret = 0;//OB_SUCCESS;
  if (IS_INIT) { reset(); }
  int64_t serialize_column_cnt = 0;
  if (nullptr == cluster_buf || !info_mask.is_valid()) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), KP(cluster_buf), K(cluster_len), K(info_mask));
  } else if (FALSE_IT(serialize_column_cnt = info_mask.is_sparse_row() ? info_mask.get_sparse_column_count() : cluster_col_cnt)) {
  } else if (info_mask.get_total_array_size(serialize_column_cnt) >= cluster_len) {
    ret = -2;//OB_SIZE_OVERFLOW;
    // LOG_WARN("invalid cluster reader argument", K(ret), K(info_mask), K(cluster_len));
  } else {
    cluster_buf_ = cluster_buf;
    // 以下这个变量只用来print log, 可被下一个注释行替代
    const int64_t spe_val_array_size = info_mask.get_special_value_array_size(serialize_column_cnt);
    const int64_t special_val_pos = cluster_len - spe_val_array_size;
    // const int64_t special_val_pos = cluster_len - info_mask.get_special_value_array_size(serialize_column_cnt);
    special_vals_ = reinterpret_cast<const uint8_t*>(cluster_buf_ + special_val_pos);
    // LOG_WARN("check special_vals info: ", K(cluster_len), K(serialize_column_cnt), K(special_val_pos), K(special_vals_[7]));

    cell_end_pos_ = special_val_pos - info_mask.get_offset_type_len() * serialize_column_cnt;
    column_cnt_ = cluster_col_cnt;
    column_offset_ = cluster_buf_ + cell_end_pos_;
    offset_bytes_ = info_mask.get_offset_type();
    if (info_mask.is_sparse_row()) {
      is_sparse_row_ = true;
      sparse_column_cnt_ = info_mask.get_sparse_column_count();
      col_idx_bytes_ = info_mask.get_column_idx_type();
      cell_end_pos_ -= info_mask.get_column_idx_type_len() * sparse_column_cnt_;
      column_idx_array_ = cluster_buf_ + cell_end_pos_;
    }
    is_inited_ = true;
    // LOG_DEBUG("success to init cluster column reader", K(ret), K(cluster_len),
    //     KPC(this), K(cell_end_pos_), K(column_cnt_), K(sparse_column_cnt_));
  }
  return ret;
}

void KwClusterColumnReader::reset()
{
  cluster_buf_ = nullptr;
  is_sparse_row_ = false;
  offset_bytes_ = KwColClusterInfoMask::BYTES_MAX;
  col_idx_bytes_ = KwColClusterInfoMask::BYTES_MAX;
  cell_end_pos_ = 0;
  cur_idx_ = 0;
  column_offset_ = nullptr;
  column_idx_array_ = nullptr;
  is_inited_ = false;
}

int64_t KwClusterColumnReader::get_sparse_col_idx(const int64_t column_idx)
{
  int64_t idx = -1;
  int64_t col_idx = 0;
  for (int i = 0; i < sparse_column_cnt_; ++i) {
    col_idx = kw_get_offset_func[col_idx_bytes_](column_idx_array_, i);
    if (col_idx == column_idx) {
      idx = i;
      break;
    } else if (col_idx > column_idx) {
      break;
    }
  }
  return idx;
}

int KwClusterColumnReader::read_storage_datum(const int64_t column_idx, KwStorageDatum &datum)
{
    int ret = 0;//OB_SUCCESS;
    if(IS_NOT_INIT || column_idx < 0 || column_idx >= column_cnt_) { ret = -31; }
    // warn "cluster column reader is not init" or "invalid argument"
    else{
        int64_t idx = is_sparse_row_ ? get_sparse_col_idx(column_idx) : column_idx;
        if(-1 == idx) { datum.set_nop(); }
        else if (idx >= 0 && idx < column_cnt_) {
            if(0 > (ret = read_datum(idx, datum))) { 
              // LOG_WARN("read datum fail", K(ret), KP(cluster_buf_), K(cell_end_pos_), K(idx), K(column_idx)); 
            }
            else {} // debug "read_storage_datum"
        }
        else { ret = -33; } //OB_ERR_UNEXPECTED, warn "invalid idx for read datum"
    }
    return ret;
}

int KwClusterColumnReader::sequence_read_datum(const int64_t column_idx, KwStorageDatum &datum)
{
    int ret = 0;//OB_SUCCESS;
    if(IS_NOT_INIT) { ret = -111; } //OB_NOT_INIT, warn "cluster column reader is not init"
    else if(column_idx < 0 || column_idx >= column_cnt_) { ret = -2; } //OB_INVALID_ARGUMENT, warn "invalid argument"
    else{
        int64_t idx = -1;
        if(is_sparse_row_){
            if(cur_idx_ < sparse_column_cnt_){
                if(kw_get_offset_func[col_idx_bytes_](column_idx_array_, cur_idx_) == column_idx){
                    idx = cur_idx_++;
                }
            }
        }
        else { idx = column_idx; }

        if(-1 == idx) { datum.set_nop(); }
        else if(idx >= 0 && idx < column_cnt_){ if(0 > (ret = read_datum(idx, datum))) {} } // warn "read datum fail"
        else { ret = -4; } //OB_ERR_UNEXPECTED, warn "invalid idx for sequence read obj"
    }
    return ret;
}

// int ObClusterColumnReader::sequence_deep_copy_datums_of_sparse(
//     const int64_t start_idx,
//     ObStorageDatum *datums)
// {}
// int ObClusterColumnReader::sequence_deep_copy_datums_of_dense(const int64_t start_idx, KwStorageDatum *datums)
// {}
// int ObClusterColumnReader::sequence_deep_copy_datums(const int64_t start_idx, KwStorageDatum *datums)
// {}
// int ObClusterColumnReader::read_cell_with_bitmap(
//     const int64_t start_idx,
//     const ObTableReadInfo &read_info,
//     KwDatumRow &datum_row,
//     memtable::ObNopBitMap &nop_bitmap)
// {}

int KwClusterColumnReader::read_8_bytes_column(
    const char *buf, 
    const int64_t buf_len,
    KwStorageDatum &datum)
{
  int ret = 0;//OB_SUCCESS
  uint64_t value = 0;
  if (buf_len <= 0 || buf_len >= sizeof(uint64_t)) {
    ret = -5109;//OB_INVALID_ARGUMENT;
    // LOG_WARN("Invalid size of column ", K(ret), KP(buf), K(buf_len));
  } else {
    switch (buf_len) {
    case 1:
      value = reinterpret_cast<const uint8_t *>(buf)[0]; 
    break;
    case 2:
      value = reinterpret_cast<const uint16_t *>(buf)[0]; 
    break;
    case 4:
      value = reinterpret_cast<const uint32_t *>(buf)[0]; 
    break;
    default:
      ret = -52;// OB_NOT_SUPPORTED;
    //   LOG_WARN("Not supported buf_len ", KP(buf), K(buf_len), K(ret));
    }
  }
  if (0 == ret) {
    datum.reuse(); 
    datum.set_uint(value);
    // LOG_DEBUG("ObClusterColumnReader read 8 bytes column ", K(value));
  } 
  return ret;
}

int KwClusterColumnReader::read_column_from_buf(
    const int64_t tmp_pos,
    const int64_t next_pos,
    const KwRowHeader::SPECIAL_VAL special_val,
    KwStorageDatum &datum) 
{
  int ret = 0;//OB_SUCCESS
  const char *buf = cluster_buf_ + tmp_pos; 
  const int64_t buf_len = next_pos - tmp_pos;
  if (special_val == KwRowHeader::VAL_ENCODING_NORMAL) {
    if (0 > (ret = read_8_bytes_column(buf, buf_len, datum))) {
      // LOG_WARN("failed to decode 8 bytes column", K(ret), K(special_val), KP(buf), K(buf_len));  
    } 
  } else if (0 > datum.from_buf_enhance(buf, buf_len)) { ret = -42;
    // LOG_WARN("failed to copy datum", K(ret), K(special_val), KP(buf), K(buf_len), KPC(this));
  } 
  return ret;
}

int KwClusterColumnReader::read_datum(const int64_t column_idx, KwStorageDatum &datum)
{
    int ret = 0;
    KwRowHeader::SPECIAL_VAL special_val = (KwRowHeader::SPECIAL_VAL)read_special_value(column_idx);
    if(KwRowHeader::VAL_NOP == special_val) { datum.set_nop(); }
    else if(KwRowHeader::VAL_NULL == special_val) { datum.set_null(); }
    else if(KwRowHeader::VAL_NORMAL != special_val && KwRowHeader::VAL_ENCODING_NORMAL != special_val) { ret = -41; } //OB_ERR_UNEXPECTED, warn "unexpected specail val"
    else{
        int64_t next_pos = -1;
        int64_t tmp_pos = kw_get_offset_func[offset_bytes_](column_offset_, column_idx);
        if (column_idx + 1 < (is_sparse_row_ ? sparse_column_cnt_ : column_cnt_)) {
            next_pos = kw_get_offset_func[offset_bytes_](column_offset_, column_idx + 1);
        }
        else{ // cur cell is the last cell
            next_pos = cell_end_pos_;
        }
        if(0 > (ret = read_column_from_buf(tmp_pos, next_pos, special_val, datum))) { 
            // LOG_WARN("failed to read column from buf", K(ret), K(column_idx), K(tmp_pos), K(next_pos), K(cell_end_pos_), K(special_val), K(column_offset_), K(offset_bytes_), K(special_vals_), K(special_vals_[7])); 
        }
        // std::cout << "\nCol type: " << ;
        // else { LOG_WARN("succeed to read column from buf", K(ret), K(column_idx), K(tmp_pos), K(next_pos), K(cell_end_pos_), K(special_val), K(column_offset_), K(offset_bytes_)); }
    }
    return ret;
}



/********************KwRowReaderV2***********************/

KwRowReader::KwRowReader()
    : buf_(NULL),
      row_len_(0),
      row_header_(NULL),
      cluster_offset_(NULL),
      column_offset_(NULL),
      column_idx_array_(NULL),
      cluster_reader_(),
      cur_read_cluster_idx_(-1),
      cluster_cnt_(0),
      rowkey_independent_cluster_(false),
      is_setuped_(false)
{}

void KwRowReader::reset()
{
    buf_ = NULL;
    row_len_ = 0;
    row_header_ = NULL;
    cluster_offset_ = NULL;
    column_offset_ = NULL;
    column_idx_array_ = NULL;
    cluster_reader_.reset();
    cur_read_cluster_idx_ = -1;
    cluster_cnt_ = 0;
    rowkey_independent_cluster_ = false;
    is_setuped_ = false;
}

bool KwRowReader::is_valid() const
{
  return is_setuped_ && NULL != row_header_  && row_header_->is_valid() && NULL != buf_ && row_len_ > 0;
}


inline int KwRowReader::setup_row(const char *buf, const int64_t row_len)
{
    int ret = 0; 
    if(nullptr == buf || row_len < 0) { ret = -1; } //OB_INVALID_ARGUMENT, warn "invalid row reader argument"
    else{// set all position
        buf_ = buf;
        row_len_ = row_len;
        cur_read_cluster_idx_ = -1;
        if(0 > analyze_row_header()) { ret = -2; } // warn "invalid row reader argument."
        else { is_setuped_ = true; }
    }
    return ret;
}

int KwRowReader::read_row_header(const char *row_buf, const int64_t row_len, const KwRowHeader *&row_header)
{
    int ret = 0;
    row_header = nullptr;
    if(nullptr == row_buf || row_len < sizeof(KwRowHeader)) { ret = -1; } //OB_INVALID_ARGUMENT, warn "invalid row reader argument"
    else { row_header = reinterpret_cast<const KwRowHeader*>(row_buf); } // get NewRowHeader
    return ret;
}

// int KwRowReader::read_memtable_row(
//     const char *row_buf,
//     const int64_t row_len,
//     const ObTableReadInfo &read_info,
//     KwDatumRow &datum_row,
//     memtable::ObNopBitMap &nop_bitmap,
//     bool &read_finished)
// {}

/*
 * Row with cluster : KwRowHeader | Column Cluster | Cluster Offset Array
 *       Column Cluster: ClusterInfoMask | Column Array | Column Offset Array
 * Row without cluster : KwRowHeader | Column Array | Column Offset Array
 * */
inline int KwRowReader::analyze_row_header()
{
    int ret = 0;//OB_SUCCESS
    row_header_ = reinterpret_cast<const KwRowHeader*>(buf_); // get NewRowHeader
    if(!row_header_->is_valid()) { ret = -1; } // warn "row header is invalid"
    else if(0 > analyze_cluster_info()) { ret = -2; } // warn "failed to analyze cluster info"
    return ret;
}

inline int KwRowReader::analyze_cluster_info()
{
    int ret = 0;//OB_SUCCESS
    rowkey_independent_cluster_ = row_header_->get_rowkey_count() >= 32LL && row_header_->get_rowkey_count() <= 48LL; //row_header_->has_rowkey_independent_cluster();
    cluster_cnt_ = row_header_->get_cluster_cnt(); //目前默认就1个cluster
    const int64_t cluster_offset_len = row_header_->get_offset_type_len() * cluster_cnt_;
    if(KwRowHeader::get_serialized_size() + cluster_offset_len >= row_len_) {
        ret = -1; // warn "invalid row reader argument"
        row_header_ = NULL;
    } 
    else {
        cluster_offset_ = buf_ + row_len_ - cluster_offset_len;
        //debug "analyze cluster info"
    }
    return ret;
}

uint64_t KwRowReader::get_cluster_offset(const int64_t cluster_idx) const
{
  return cluster_idx == 0 ? sizeof(KwRowHeader) :
             kw_get_offset_func[row_header_->get_offset_type()](cluster_offset_, cluster_idx);
}

uint64_t KwRowReader::get_cluster_end_pos(const int64_t cluster_idx) const
{
  return cluster_cnt_ - 1 == cluster_idx ? row_len_ - row_header_->get_offset_type_len() * cluster_cnt_:
          kw_get_offset_func[row_header_->get_offset_type()](cluster_offset_, cluster_idx + 1);
}

int KwRowReader::analyze_info_and_init_reader(const int64_t cluster_idx)
{
  int ret = 0;//OB_SUCCESS;
  if (cluster_idx < 0 || cluster_idx > cluster_cnt_) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), KPC(row_header_), K(cluster_idx));
  } else if (cur_read_cluster_idx_ != cluster_idx) { // need init another ClusterReader
    cluster_reader_.reset();
    const uint64_t cluster_start_pos = get_cluster_offset(cluster_idx);
    const KwColClusterInfoMask *info_mask = reinterpret_cast<const KwColClusterInfoMask*>(buf_ + cluster_start_pos);
    if (0 > (cluster_reader_.init(
            buf_ + cluster_start_pos,
            get_cluster_end_pos(cluster_idx) - cluster_start_pos, // cluster len
            row_header_->is_single_cluster() ? row_header_->get_column_count() : info_mask->get_column_count(),
            *info_mask))) { ret = -2;
    //   LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_), K(cluster_idx));
    } else {
      // only rowkey independent cluster have columns more than CLUSTER_COLUMN_CNT, but it can't be sparse
      cur_read_cluster_idx_ = cluster_idx;
    //   LOG_DEBUG("success to init cluster reader", K(cluster_idx), KPC(info_mask), KPC(row_header_),
    //       K(cluster_start_pos), K(get_cluster_end_pos(cluster_idx)));
    }
  }
  return ret;
}

int KwRowReader::read_row(
    const char *row_buf,
    const int64_t row_len,
    const KwTableReadInfo *read_info,
    KwDatumRow &datum_row)
{
    int ret = 0;
    // set position and get row header
    if(nullptr != read_info && !read_info->is_valid()) { ret = -10; } //OB_INVALID_ARGUMENT, warn "Invalid argument to read row"
    else if(0 > setup_row(row_buf, row_len)) { ret = -2; } // warn "Fail to set up row"
    else {
        int64_t seq_read_cnt = 0;
        int64_t column_cnt = 0;
        if(nullptr == read_info) { seq_read_cnt = column_cnt = row_header_->get_column_count(); }
        else{
            seq_read_cnt = read_info->get_seq_read_column_count();
            column_cnt = read_info->get_request_count();
        }

        if(datum_row.is_valid()){ if(0 > datum_row.reserve(column_cnt)) { ret = -3; }} // warn "Failed to reserve datum row"
        else if(0 > datum_row.init(column_cnt)) { ret = -4; } // warn "Failed to init datum row"

        if(0 == ret){
            SET_ROW_BASIC_INFO(datum_row);  // set flag/row_type_flag/trans_id
            datum_row.count_ = column_cnt;
            // sequence read
            //默认1个cluster的情况下：
            int64_t idx = 0;
            if (seq_read_cnt > 0) {
                int64_t cluster_col_cnt = 0;
                for (int64_t cluster_idx = 0; 0 == ret && cluster_idx < row_header_->get_cluster_cnt() && idx < seq_read_cnt; ++cluster_idx) {
                    if (0 > analyze_info_and_init_reader(cluster_idx)) { ret = -5; } // warn "failed to init cluster column reader"
                    else {
                        cluster_col_cnt = cluster_reader_.get_column_count();
                        for (int64_t i = 0; 0 == ret && i < cluster_col_cnt && idx < seq_read_cnt; ++idx, ++i) {
                            if (i >= row_header_->get_column_count()) { datum_row.storage_datums_[i].set_nop(); }// not exists
                            else if (0 > (ret = cluster_reader_.sequence_read_datum(i, datum_row.storage_datums_[idx]))) {} // warn "Fail to read column"
                            else {} // debug "sequence read datum"
                        }
                    }
                } // end of for
            }

            if(nullptr != read_info){
                int64_t store_idx = 0;
                const common::ObIArray<int32_t> &cols_index = read_info->get_columns_index();
                // const ObColumnIndexArray &cols_index = read_info->get_columns_index();
                for(; 0 == ret && idx < read_info->get_request_count(); ++idx){ // loop the ColumnIndex array
                    store_idx = cols_index.at(idx);
                    uint16_t col_cnt = row_header_->get_column_count();
                    if(store_idx < 0 || store_idx >= col_cnt) { 
                      datum_row.storage_datums_[idx].set_nop(); 
                    } // not exists
                    // if(store_idx < 0 || store_idx >= row_header_->get_column_count()) { datum_row.storage_datums_[idx].set_nop(); } // not exists
                    else if(0 > (ret = read_specific_column_in_cluster(store_idx, datum_row.storage_datums_[idx]))) {
                      // LOG_WARN("failed to read datum from cluster column reader", K(ret), K(store_idx));
                    }
                } // end of for
            }
        }
    }
    reset();
    return ret;
}

int KwRowReader::read_column(
    const char *row_buf,
    const int64_t row_len,
    const int64_t col_idx,
    KwStorageDatum &datum)
{
  int ret = 0;
  if (0 > setup_row(row_buf, row_len)) { ret = -1;
    // LOG_WARN("failed to setup row", K(ret), K(row_buf), K(row_len));
  } else if (col_idx < 0 || col_idx >= row_header_->get_column_count()) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), K(col_idx));
  } else if (0 > read_specific_column_in_cluster(col_idx, datum)) { ret = -3;
    // LOG_WARN("failed to read obj from cluster column reader", K(ret), KPC(row_header_), K(col_idx));
  }
  return ret;
}

int KwRowReader::read_specific_column_in_cluster(const int64_t store_idx, KwStorageDatum &datum)
{
    int ret = 0;
    int64_t cluster_idx = 0;
    int64_t col_idx_in_cluster = 0;
    if (row_header_->is_single_cluster()) {
        cluster_idx = 0;
        col_idx_in_cluster = store_idx;
    } else if (!rowkey_independent_cluster_) {
        cluster_idx = KwRowHeader::calc_cluster_idx(store_idx);
        col_idx_in_cluster = KwRowHeader::calc_column_idx_in_cluster(store_idx);
    } else if (store_idx < row_header_->get_rowkey_count()) { // rowkey independent
        cluster_idx = 0;
        col_idx_in_cluster = store_idx;
    } else {
        int64_t idx = store_idx - row_header_->get_rowkey_count();
        cluster_idx = KwRowHeader::calc_cluster_idx(idx) + 1;
        col_idx_in_cluster = KwRowHeader::calc_column_idx_in_cluster(idx);
    }
    // LOG_DEBUG("DEBUG cluster reader", K(cluster_idx), K(col_idx_in_cluster), K(cur_read_cluster_idx_));
    if (0 > (ret = analyze_info_and_init_reader(cluster_idx))) { //ret = -21;
        // LOG_WARN("failed to init cluster column reader", K(ret), KPC(row_header_),
        //     K(cluster_idx), K(col_idx_in_cluster));
    } else if (0 > (ret = cluster_reader_.read_storage_datum(col_idx_in_cluster, datum))) {
        // LOG_WARN("failed to read datum from cluster column reader", K(ret), K(cluster_idx), K(col_idx_in_cluster));
    }
    return ret;
}

// int KwRowReader::read_char(
//     const char* buf,
//     int64_t end_pos,
//     int64_t &pos,
//     ObString &value)
// {}

// called by KwIMicroBlockFlatReader::find_bound_::PreciseCompare
int KwRowReader::compare_meta_rowkey(
    const KwDatumRowkey &rhs,
    const KwTableReadInfo &read_info,
    const char *buf,
    const int64_t row_len,
    int32_t &cmp_result)
{
    int ret = 0;//OB_SUCCESS
    if(!rhs.is_valid() || !read_info.is_valid() || nullptr == buf || row_len <= 0) { ret = -1; } // OB_INVALID_ARGUMENT
    else{
        cmp_result = 0;
        const int64_t compare_column_count = rhs.get_datum_cnt();
        const KwStorageDatumUtils &datum_utils = read_info.get_datum_utils();
        // const KwStorageDatumUtils &datum_utils = KwStorageDatumUtils::from_ob(ob_datum_utils.get_rowkey_count(), ob_datum_utils.get_column_count(), ob_datum_utils.is_oracle_mode(), ob_datum_utils.is_valid());
        if(datum_utils.get_rowkey_count() < compare_column_count) { ret = -2; } // OB_ERR_UNEXPECTED
        else if(0 > (ret = setup_row(buf, row_len))) {} // warn "row reader fail to setup."
        else if(row_header_->get_rowkey_count() < compare_column_count) { ret = -4; } // OB_ERR_UNEXPECTED
        else {
            KwStorageDatum datum;
            int64_t cluster_col_cnt = 0;
            int64_t idx = 0;
            for(int64_t cluster_idx = 0; 0 == ret && cmp_result == 0 && cluster_idx < row_header_->get_cluster_cnt() && idx < compare_column_count; ++cluster_idx) {
                if(0 > (ret = analyze_info_and_init_reader(cluster_idx))) {} // warn "Fail to read column"
                else if(0 > (ret = datum_utils.get_cmp_funcs().at(idx).compare(datum, rhs.datums_[idx], cmp_result))) {} //warn "Failed to compare datums"
                // LOG_DEBUG
            }
        }
    }
    reset();
    return ret;
}


} // namespace blocksstable
} // namespace oceanbase