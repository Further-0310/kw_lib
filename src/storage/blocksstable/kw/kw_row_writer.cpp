#define USING_LOG_PREFIX STORAGE
#include "kw_row_writer.h"
// #include "storage/ob_i_store.h"

using namespace oceanbase;
using namespace common;
using namespace blocksstable;

KwRowWriter::KwRowWriter()
    :   buf_(NULL),
        buf_size_(0),
        start_pos_(0),
        pos_(0),
        row_header_(NULL),
        column_index_count_(0),
        rowkey_column_cnt_(0),
        update_idx_array_(nullptr),
        update_array_idx_(0),
        cluster_cnt_(0),
        row_buffer_()
{
    static_assert(sizeof(KwBitArray<48>) < 400, "row buffer size");
    static_assert(sizeof(KwRowWriter) < 6100, "row buffer size");
}

KwRowWriter::~KwRowWriter() { reset(); }

void KwRowWriter::reset()
{
    buf_ = nullptr;
    buf_size_ = 0;
    start_pos_ = 0;
    pos_ = 0;
    row_header_ = nullptr;
    column_index_count_ = 0;
    column_index_count_ = 0;
    update_idx_array_ = nullptr;
    update_array_idx_ = 0;
    cluster_cnt_ = 0;
    //row_buffer_.reset(); row_buffer_ maybe do not need reset
}


int KwRowWriter::init_common(char *buf, const int64_t buf_size, const int64_t pos)
{
  int ret = 0;//OB_SUCCESS
  if(NULL == buf || buf_size <= 0 || pos < 0 || pos >= buf_size){ ret = -1; } //OB_INVALID_ARGUMENT, warn "invalid row writer input argument"
  else {
    buf_ = buf;
    buf_size_ = buf_size;
    start_pos_ = pos;
    pos_ = pos;
    row_header_ = NULL;
    column_index_count_ = 0;
    update_array_idx_ = 0;
  }
  return ret;
}

// int KwRowWriter::check_row_valid(const ObStoreRow &row, const int64_t rowkey_column_count)
// {
//     int ret = 0; // OB_SUCCESS;
//     if(!row.is_valid()) { ret = -1; } // OB_INVALID_ARGUMENT
//     else if(rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) { ret = -2; } //OB_INVALID_ARGUMENT
//     return ret;
// }

int KwRowWriter::append_row_header(
    const uint8_t row_flag,
    const uint8_t multi_version_flag,
    const int64_t trans_id,
    const int64_t column_cnt,
    const int64_t rowkey_cnt)
{
  int ret = 0;//OB_SUCCESS
//   const int64_t row_header_size = KwRowHeader::get_serialized_size();
  const int64_t row_header_size = KwRowHeader::get_serialized_size();
  if(pos_ + row_header_size > buf_size_){
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not enough", K(ret), K(pos_), K(buf_size_));
  }
  else {
    row_header_ = reinterpret_cast<KwRowHeader*>(buf_ + pos_);
    row_header_->set_version(KwRowHeader::ROW_HEADER_VERSION_1);
    row_header_->set_row_flag(row_flag);
    row_header_->set_row_mvcc_flag(multi_version_flag);
    row_header_->set_column_count(column_cnt);
    row_header_->set_rowkey_count(rowkey_cnt);
    row_header_->clear_reserved_bits();
    row_header_->set_trans_id(trans_id); // TransID

    pos_ += row_header_size; // move forward
  }
  return ret;
}

int KwRowWriter::write(
  const int64_t rowkey_column_count,
  const KwDatumRow &row,
  char *buf,
  const int64_t buf_size,
  int64_t &pos)
{
  int ret = 0;//OB_SUCCESS
  if(0 > init_common(buf, buf_size, pos)) { ret = -1; } // warn "row writer fail to init common"
  else if(0 > append_row_header(
    row.row_flag_.get_serialize_flag(),
    row.mvcc_row_flag_.flag_,
    row.trans_id_,
    row.count_,
    rowkey_column_count)){ ret = -2; } // warn "row writer fail to append row header"
  else {
    update_idx_array_ = nullptr;
    rowkey_column_cnt_ = rowkey_column_count;
    if(0 > inner_write_cells(row.storage_datums_, row.count_)){ ret = -3; } // warn "failed to write datums"
    else { pos = pos_; } // debug "write row"
  }
  return ret;
}

int KwRowWriter::alloc_buf_and_init(const bool retry)
{
  int ret = 0;//OB_SUCCESS;
  if (retry && 0 > row_buffer_.extend_buf()) { ret = -1;
    // STORAGE_LOG(WARN, "Failed to extend buffer", K(ret), K(row_buffer_));
  } else if (0 > init_common(row_buffer_.get_buf(), row_buffer_.get_buf_size(), 0)) { ret = -2;
    // LOG_WARN("row writer fail to init common", K(ret), K(row_buffer_));
  }
  return ret;
}

// int KwRowWriter::write_rowkey(const common::ObStoreRowkey &rowkey, char *&buf, int64_t &len)
// {
//   int ret = 0;//OB_SUCCESS;
//   len = 0;
//   do {
//     if (0 > alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret)) {
//       LOG_WARN("row writer fail to alloc and init", K(ret));
//     } else if (0 > append_row_header(0, 0, 0, rowkey.get_obj_cnt(), rowkey.get_obj_cnt())) { ret = -2;
//     //   LOG_WARN("row writer fail to append row header", K(ret));
//     } else {
//       update_idx_array_ = nullptr;
//       rowkey_column_cnt_ = rowkey.get_obj_cnt();
//       if (0 > (ret = inner_write_cells(rowkey.get_obj_ptr(), rowkey.get_obj_cnt()))) {
//         if (OB_BUF_NOT_ENOUGH != ret) {
//           LOG_WARN("failed to write cells", K(ret), K(rowkey));
//         }
//       } else {
//         buf = row_buffer_.get_buf();
//         len = pos_;
//         // LOG_DEBUG("finish write rowkey", K(ret), KPC(row_header_), K(rowkey));
//       }
//     }
//   } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

//   if (OB_BUF_NOT_ENOUGH == ret) {
//     LOG_WARN("fail to append row due to buffer not enough", K(ret), K(row_buffer_), K(rowkey));
//   }
//   return ret;
// }

int KwRowWriter::write(const int64_t rowkey_column_cnt, const KwDatumRow &datum_row, char *&buf, int64_t &len)
{
  int ret = 0;//OB_SUCCESS;
  len = 0;
  do {
    if (0 > (ret = alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) {
      LOG_WARN("row writer fail to alloc and init", K(ret));
    } else if (0 > (ret = append_row_header(
            datum_row.row_flag_.get_serialize_flag(),
            datum_row.mvcc_row_flag_.flag_,
            // datum_row.trans_id_.get_id(),
            datum_row.get_trans_id(),
            datum_row.count_,
            rowkey_column_cnt))) { 
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("row writer fail to append row header", K(ret)/*, K(datum_row)*/);
      }
    } else {
      update_idx_array_ = nullptr;
      rowkey_column_cnt_ = rowkey_column_cnt;
      if (0 > (ret = inner_write_cells(datum_row.storage_datums_, datum_row.count_))) { //ret = -3;
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("failed to write datums", K(ret)/*, K(datum_row)*/);
        }
      } else {
        len = pos_;
        buf = row_buffer_.get_buf();
        // LOG_DEBUG("finish write row", K(ret), KPC(row_header_), K(datum_row));
      }
    }
  } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

  if (OB_BUF_NOT_ENOUGH == ret) {
    LOG_WARN("fail to append row due to buffer not enough", K(ret), K(row_buffer_), /*K(datum_row), */K(rowkey_column_cnt));
  }
  return ret;
}

// when update_idx == nullptr, write full row; else only write rowkey + update cells
// int KwRowWriter::write(
//     const int64_t rowkey_column_count,
//     const storage::ObStoreRow &row,
//     const ObIArray<int64_t> *update_idx,
//     char *&buf,
//     int64_t &len)
// {
//   int ret = 0;//OB_SUCCESS;
//   len = 0;
//   if (nullptr != update_idx && update_idx->count() > row.row_val_.count_) {
//     ret = -1;//OB_INVALID_ARGUMENT;
//     // LOG_WARN("update idx is invalid", K(ret), KPC(update_idx), K(row), K(rowkey_column_count));
//   } else {
//     do {
//       if (0 > (ret = alloc_buf_and_init(OB_BUF_NOT_ENOUGH == ret))) { //ret = -2;
//         // LOG_WARN("row writer fail to alloc and init", K(ret));
//       } else if (0 > (ret = inner_write_row(rowkey_column_count, row, update_idx))) { //ret = -3;
//         if (OB_BUF_NOT_ENOUGH != ret) {
//           LOG_WARN("row writer fail to append row header", K(ret), K(row_buffer_), K(pos_));
//         }
//       } else {
//         len = pos_;
//         buf = row_buffer_.get_buf();
//       }
//     } while (OB_BUF_NOT_ENOUGH == ret && row_buffer_.is_buf_extendable());

//     if (OB_BUF_NOT_ENOUGH == ret) {
//       LOG_WARN("fail to append row due to buffer not enough", K(ret), K(row_buffer_), /*K(row), */K(rowkey_column_count));
//     }
//   }
//   return ret;
// }

int KwRowWriter::check_update_idx_array_valid(
    const int64_t rowkey_column_count,
    const ObIArray<int64_t> *update_idx)
{
  int ret = 0;//OB_SUCCESS;
  if (rowkey_column_count <= 0 || nullptr == update_idx) {
    ret = -1234;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), K(rowkey_column_count), KPC(update_idx));
  }
  for (int i = 0; 0 == ret && i < update_idx->count(); ++i) {
    if (i > 0) {
      if (update_idx->at(i) <= update_idx->at(i - 1)) {
        ret = -2345;//OB_ERR_UNEXPECTED;
        // LOG_WARN("update idx array is invalid", K(ret), K(i), KPC(update_idx));
      }
    } else if (update_idx->at(i) < rowkey_column_count) { // i == 0
      ret = -3456;//OB_ERR_UNEXPECTED;
    //   LOG_WARN("update idx array is invalid", K(ret), K(i), KPC(update_idx));
    }
  }
  return ret;
}

// int KwRowWriter::inner_write_row(
//     const int64_t rowkey_column_count,
//     const ObStoreRow &row,
//     const ObIArray<int64_t> *update_idx)
// {
//   int ret = 0;//OB_SUCCESS;
//   if (0 > (ret = check_row_valid(row, rowkey_column_count))) { //ret = -1;
//     // LOG_WARN("row writer fail to init store row", K(ret), K(rowkey_column_count));
//   } else if (nullptr != update_idx && 0 > (ret = check_update_idx_array_valid(rowkey_column_count, update_idx))) { //ret = -2;
//     // LOG_WARN("invalid update idx array", K(ret));
//   } else if (0 > (ret = append_row_header(
//           row.flag_.get_serialize_flag(),
//           row.row_type_flag_.flag_,
//           row.trans_id_.get_id(),
//           row.row_val_.count_,
//           rowkey_column_count))) { //ret = -3;
//     if (OB_BUF_NOT_ENOUGH != ret) {
//       LOG_WARN("row writer fail to append row header", K(ret), K(row));
//     }
//   } else {
//     update_idx_array_ = update_idx;
//     rowkey_column_cnt_ = rowkey_column_count;
//     if (0 > (ret = inner_write_cells(row.row_val_.cells_, row.row_val_.count_))) { //ret = -4;
//       if (OB_BUF_NOT_ENOUGH != ret) {
//         LOG_WARN("failed to write cells", K(ret), K(row));
//       }
//     }
//   }
//   return ret;
// }

template <typename T>
int KwRowWriter::inner_write_cells(
    const T * cells,
    const int64_t cell_cnt)
{
  int ret = 0;//OB_SUCCESS
  if(cell_cnt <= KwRowHeader::USE_CLUSTER_COLUMN_COUNT){ 
    cluster_cnt_ = 1;
    use_sparse_row_[0] = false;
  }
  else{ loop_cells(cells, cell_cnt, cluster_cnt_, use_sparse_row_); }

  if(cluster_cnt_ >= MAX_CLUSTER_CNT || (cluster_cnt_ > 1 && cluster_cnt_ != KwRowHeader::calc_cluster_cnt(rowkey_column_cnt_, cell_cnt))) { ret = -1; } // OB_ERR_UNEXPECTED
  else if(0 > (ret = build_cluster(cell_cnt, cells))) {
    if(OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to build cluster", K(ret));
    }
  }
  else{
    row_header_->set_single_cluster(1 == cluster_cnt_);
    // LOG_DEBUG("inner_write_row", K(ret), KPC(row_header_), K(use_sparse_row_[0]), K(column_index_count_), K(pos_));
  }

  return ret;
}

bool KwRowWriter::check_col_exist(const int64_t col_idx)
{
    bool bret = false;
    if(nullptr == update_idx_array_){ bret = true; }
    else if(update_array_idx_ < update_idx_array_->count()
      && col_idx == update_idx_array_->at(update_array_idx_)){
        update_array_idx_++;
        bret = true;
    }
    else if(col_idx < rowkey_column_cnt_){ bret = true; }
    return bret;
}

/*省略cluster相关函数实现*/
// template<typename T, typename R>
// int KwRowWriter::append_row_and_index(
//     const T *cells,
//     const int64_t offset_start_pos,
//     const int64_t start_idx,
//     const int64_t end_idx,
//     const bool is_sparse_row,
//     R &bytes_info)
// {}

template<typename T>
int KwRowWriter::write_col_in_cluster(
    const T *cells,
    const int64_t cluster_idx,
    const int64_t start_col_idx,
    const int64_t end_col_idx)
{
  int ret = 0;//OB_SUCCESS;
  if(cluster_idx < 0 || start_col_idx < 0 || start_col_idx > end_col_idx) { ret = -1; } // OB_INVALID_ARGUMENT
  else if(pos_ + KwColClusterInfoMask::get_serialized_size() > buf_size_) { ret = OB_BUF_NOT_ENOUGH; }
  else {// serialize info mask for current cluster
    const int64_t offset_start_pos = pos_;
    KwColClusterInfoMask *info_mask = reinterpret_cast<KwColClusterInfoMask*>(buf_ + pos_);
    pos_ += KwColClusterInfoMask::get_serialized_size();
    info_mask->reset();
    
    //append_row_and_index()：
    column_index_count_ = 0; // set the column_index & column_idx array empty
    KwColClusterInfoMask::BYTES_LEN offset_type = KwColClusterInfoMask::BYTES_MAX;
    if(use_sparse_row_[cluster_idx/*0*/]){//sparse，原本判断稀疏在use_sparse_row_中，现在先用row header的来判断
        KwColClusterInfoMask::BYTES_LEN col_idx_type = KwColClusterInfoMask::BYTES_MAX;
        // append_sparse_cell_array:
        int64_t column_offset = 0;
        bool is_nop_val = false;
        for(int64_t i = start_col_idx; 0 == ret && i < end_col_idx; ++i){
            is_nop_val = false;
            if(!check_col_exist(i)){ is_nop_val = true; }// 不该被序列化的列
            else if(cells[i].is_ext()){ //is nop
              if(!cells[i].is_nop_value()){ ret = -111; }//不支持的扩展类型
              else{ is_nop_val = true; }
            }
            else{
              column_offset = pos_ - offset_start_pos;
              if(cells[i].is_null()){
                special_vals_[column_index_count_] = KwRowHeader::VAL_NULL;
              }
              else{
                special_vals_[column_index_count_] = cells[i].is_outrow() ? KwRowHeader::VAL_OUTROW : KwRowHeader::VAL_NORMAL;
                if(0 > (ret = append_column(cells[i]))){
                    if (OB_BUF_NOT_ENOUGH != ret) {
                      LOG_WARN("row writer fail to append column", K(ret), K(i), K(cells[i]));
                    }
                    break;
                }
              }
            }
            if(0 == ret && !is_nop_val){
              column_offset_.set_val(column_index_count_, column_offset);
              column_idx_.set_val(column_index_count_, i - start_col_idx);
              // debug "append_sparse_cell_array"
              column_index_count_++;
            }
        }// end append_sparse_cell_array
        if(0 > ret && OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("row writer fail to append store row", K(ret));
        }
        if(0 == ret){
            if(0 > (ret = append_array(column_idx_, column_index_count_, col_idx_type))){
              if (OB_BUF_NOT_ENOUGH != ret) {
                LOG_WARN("failed to append column idx array", K(ret));
              }
            } //warn BUF_NOT_ENOUGH or "failed to append column idx array"
            //设置列索引类型，原本列索引类型在KwColClusterInfoMask中，现在先放到row header中
            //   row_header_->set_column_idx_type(col_idx_type);
            else if(0 > info_mask->set_column_idx_type(col_idx_type)) { ret = -31; }
        }
    }
    else{//append_flat_cell_array:
        int64_t column_offset = 0;
        for(int64_t i = start_col_idx; 0 == ret && i < end_col_idx; ++i){
          column_offset = pos_ - offset_start_pos;
          if(!check_col_exist(i)){ // col should not serialize
              special_vals_[column_index_count_] = KwRowHeader::VAL_NOP;
          }
          else if(cells[i].is_ext()){
              if(cells[i].is_nop_value()){
                special_vals_[column_index_count_] = KwRowHeader::VAL_NOP;
              } 
              else { ret = -4321; }//不支持的扩展类型
          }
          else if(cells[i].is_null()){
              special_vals_[column_index_count_] = KwRowHeader::VAL_NULL;
          }
          else{
              special_vals_[column_index_count_] = cells[i].is_outrow() ? KwRowHeader::VAL_OUTROW : KwRowHeader::VAL_NORMAL;
              if(0 > (ret = append_column(cells[i]))) {
                  if (OB_BUF_NOT_ENOUGH != ret)  {
                    LOG_WARN("row writer fail to append column", K(ret), K(i), K(cells[i]));
                  }
                  break; 
              }
              // std::cout << "\nCol " << i << " get_uint64()=" << cells[i].get_uint64() << ", to_string()=" << to_cstring(cells[i]) << std::endl;
          }
          if(0 == ret) { column_offset_.set_val(column_index_count_++, column_offset); } // debug "append_flat_cell_array"
        }// end append_flat_cell_array
        if(0 > ret && OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("row writer fail to append store row", K(ret));
        }
        if(0 == ret){
            if(0 > (ret = append_array(column_offset_, column_index_count_, offset_type))) {
              if (OB_BUF_NOT_ENOUGH != ret)  {
                LOG_WARN("row writer fail to append column index", K(ret));
              }
            } // warn BUF_NOT_ENOUGH or "failed to append column idx array" 
            //设置列索引类型
            //   row_header_->set_offset_type(offset_type);
            else if(0 > info_mask->set_offset_type(offset_type)) { ret = -61; }
            else {
                //append_special_val_array(column_index_count_):
                //flat最后添加处理特殊值的数组
                const int64_t copy_size = (sizeof(uint8_t) * column_index_count_ + 1) >> 1;
                if(pos_ + copy_size > buf_size_){ ret = -7; } // BUF_NOT_ENOUGH
                else {
                    for(int64_t i = 0; i < copy_size; i++){
                      // 一个字节表示两种特殊值。每个字节中后面的特殊值在高位。special_vals_中的每个值都必须小于0x0F
                      const int64_t low_bit_idx = i * 2;
                      const int64_t high_bit_idx = low_bit_idx + 1;
                      const uint8_t low_bit_value = special_vals_[low_bit_idx];
                      const uint8_t high_bit_value = (high_bit_idx == column_index_count_ ? 0 : special_vals_[high_bit_idx]);
                      const uint8_t packed_value = low_bit_value | (high_bit_value << 4);
                      special_vals_[i] = packed_value;
                    }
                    memcpy(buf_ + pos_, special_vals_, copy_size);
                    pos_ += copy_size;
                }
            }  // end append_special_val_array
        }
    } //end append_row_and_index

    if(0 > ret && OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to append row and index", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx));
    }
    if(0 == ret){
        info_mask->set_sparse_row_flag(use_sparse_row_[cluster_idx/*0*/]);
        info_mask->set_sparse_column_count(use_sparse_row_[cluster_idx/*0*/] ? column_index_count_ : 0);
        info_mask->set_column_count(1 == cluster_cnt_ ? UINT8_MAX : end_col_idx - start_col_idx);
        // LOG_DEBUG "after append_row_and_index"
    }
    // end write_col_in_cluster
  }

  return ret;
}

template<typename T>
int KwRowWriter::build_cluster(const int64_t col_cnt, const T *cells)
{
  int ret = 0;//OB_SUCCESS;
  //build_cluster => write_col_in_cluster => append_row_and_index
  KwColClusterInfoMask::BYTES_LEN cluster_offset_type = KwColClusterInfoMask::BYTES_MAX;
  bool rowkey_independent_cluster = KwRowHeader::need_rowkey_independent_cluster(rowkey_column_cnt_);

  int cluster_idx = 0;
  int64_t start_col_idx = 0;
  int64_t end_col_idx = 0;
  if(rowkey_independent_cluster || 1 == cluster_cnt_){
    // write rowkey cluster OR only cluster
    cluster_offset_.set_val(cluster_idx, pos_ - start_pos_);
    end_col_idx = (1 == cluster_cnt_) ? col_cnt : rowkey_column_cnt_;
    if(0 > (ret = write_col_in_cluster(cells, cluster_idx++, start_col_idx/*0*/, end_col_idx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write col in cluster", K(ret));
      } 
    }
  }
  // LOG_DEBUG
  for( ; 0 == ret && cluster_idx < cluster_cnt_; ++ cluster_idx){
    const int64_t cluster_offset = pos_ - start_pos_;
    cluster_offset_.set_val(cluster_idx, cluster_offset);
    // LOG_DEBUG
    start_col_idx = end_col_idx;
    end_col_idx += KwRowHeader::CLUSTER_COLUMN_CNT;
    if(0 > (ret = write_col_in_cluster(cells, cluster_idx, start_col_idx, MIN(end_col_idx, col_cnt)))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write col in cluster", K(ret), K(cluster_idx), K(start_col_idx), K(end_col_idx));
      } 
    }
    // LOG_DEBUG
  }

  if(0 > ret) {}
  else if(0 > (ret = append_array(cluster_offset_, cluster_cnt_, cluster_offset_type))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("row writer fail to append cluster offset array", K(ret));
    }
  }
  else if(0 > (ret = row_header_->set_offset_type(cluster_offset_type))) {
    LOG_WARN("failed to set offset bytes", K(ret));
  }
  // end build_cluster
  return ret;
}


template <typename T>
void KwRowWriter::loop_cells(
    const T *cells,
    const int64_t cell_cnt,
    int64_t &cluster_cnt,
    bool *output_sparse_row)
{
  int32_t total_nop_count = 0;
  const bool rowkey_dependent_cluster  = KwRowHeader::need_rowkey_independent_cluster(rowkey_column_cnt_);
  const bool multi_cluster = cell_cnt > KwRowHeader::USE_CLUSTER_COLUMN_COUNT;
  if (!multi_cluster) { // no cluster
    cluster_cnt = 1;
    if (rowkey_dependent_cluster) {
      output_sparse_row[0] = false;
    } else {
      if (nullptr != update_idx_array_) {
        output_sparse_row[0] = (cell_cnt - update_idx_array_->count() + rowkey_column_cnt_) >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      } else { // no update idx array, need loop row
        for (int i = rowkey_column_cnt_; i < cell_cnt; ++i) {
          if (cells[i].is_nop_value()) {
            ++total_nop_count;
          }
        }
        output_sparse_row[0] = total_nop_count >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      }
    }
  } else { // no update idx array, need loop row
    int64_t idx = 0;
    int32_t cluster_idx = 0;
    int64_t update_idx = 0;
    if (rowkey_dependent_cluster) { // for rowkey cluster
      output_sparse_row[cluster_idx++] = false;
      idx = rowkey_column_cnt_;
    }
    while (idx < cell_cnt) {
      int32_t tmp_nop_count = 0;
      for (int i = 0; i < KwRowHeader::CLUSTER_COLUMN_CNT && idx < cell_cnt; ++i, ++idx) {
        if (cells[idx].is_nop_value()) {
          tmp_nop_count++;
        } else if (nullptr != update_idx_array_) {
          if (idx < rowkey_column_cnt_) {
            // rowkey col
          } else if (update_idx < update_idx_array_->count() && idx == update_idx_array_->at(update_idx)) {
            update_idx++;
          } else {
            tmp_nop_count++;
          }
        }
      }
      output_sparse_row[cluster_idx++] = tmp_nop_count >= USE_SPARSE_NOP_CNT_IN_CLUSTER;
      total_nop_count += tmp_nop_count;
    } // end of while

    if ((cell_cnt - total_nop_count) <= KwColClusterInfoMask::MAX_SPARSE_COL_CNT
         && cell_cnt < KwRowHeader::MAX_CLUSTER_COLUMN_CNT) {
      // only few non-nop columns in whole row
      cluster_cnt = 1;
      output_sparse_row[0] = true;
    } else {
      cluster_cnt = cluster_idx;
    }
  }
}

template <int64_t MAX_CNT>
inline int KwRowWriter::append_array(
    KwBitArray<MAX_CNT> &bit_array,
    const int64_t count,
    KwColClusterInfoMask::BYTES_LEN &type)
{
  int ret = 0;
  int64_t bytes = 0;
  if(0 == count){
    type = KwColClusterInfoMask::BYTES_ZERO;
  }
  else{
    uint32_t largest_val = bit_array.get_val(count - 1);
    if(0 > (ret = KwRowWriter::get_uint_byte(largest_val, bytes))) {
      LOG_WARN("fail to get column_index_bytes", K(ret), K(largest_val));
    } else{
      const int64_t copy_size = bytes * count;
      if(pos_ + copy_size > buf_size_){ ret = OB_BUF_NOT_ENOUGH; } // BUF_NOT_ENOUGH
      else{
        type = (KwColClusterInfoMask::BYTES_LEN)((bytes >> 1) + 1);
        memcpy(buf_ + pos_, bit_array.get_bit_array_ptr(type), copy_size);
        pos_ += copy_size;
      }
    }
  }
  return ret;
}

int KwRowWriter::append_column(const KwStorageDatum &datum)
{
  int ret = 0;//OB_SUCCESS;
  if(pos_ + datum.len_ > buf_size_){ ret = OB_BUF_NOT_ENOUGH; } // BUF_NOT_ENOUGH
  else if(datum.len_ == sizeof(uint64_t)){
    //append_8_bytes_column(datum):
    int64_t bytes = 0;
    uint64_t value = datum.get_uint64();
    if(0 > (ret = get_uint_byte(value, bytes))) {
      LOG_WARN("failed to get byte size", K(value), K(ret));
    } else if (bytes <= 0 || bytes > sizeof(uint64_t)) { 
      ret = -3;//OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected byte size", K(ret), K(bytes));
    } else {
      if(bytes == sizeof(uint64_t)){
        memcpy(buf_ + pos_, datum.ptr_, datum.len_);
        pos_ += datum.len_;
      }
      else{//write_uint(value, bytes)
        switch (bytes)
        {
          case 1:{
            *(reinterpret_cast<uint8_t *>(buf_ + pos_)) = static_cast<uint8_t>(value);
            break;
          }
          case 2:{
            *(reinterpret_cast<uint16_t *>(buf_ + pos_)) = static_cast<uint16_t>(value);
            break;
          }
          case 4:{
            *(reinterpret_cast<uint32_t *>(buf_ + pos_)) = static_cast<uint32_t>(value);
            break;
          }
          default:{
            //log error "没有支持的值类型"
            ret = -4;
          }
        }
        if(0 == ret) { 
          pos_ += bytes; 
          // end write_uint
          if(KwRowHeader::VAL_NORMAL != special_vals_[column_index_count_]) {
            ret = -5110;//OB_ERR_UNEXPECTED;
            uint32_t print_special_value = special_vals_[column_index_count_];
            LOG_WARN("Only normal column might be encoded ", K(ret), K(print_special_value), K(column_index_count_));
          }       
          else { 
            special_vals_[column_index_count_] = KwRowHeader::VAL_ENCODING_NORMAL;
            // LOG_DEBUG("ObRowWriter write 8 bytes value ", K(value), K(bytes));
          }  
        }
      } 
    }
  }// end append_8_bytes_column
  else{
    memcpy(buf_ + pos_, datum.ptr_, datum.len_);
    pos_ += datum.len_;
  }
  return ret;
}

int KwRowWriter::append_column(const ObObj &obj)
{
  int ret = 0;//OB_SUCCESS;
  KwStorageDatum datum;
  if (0 > (ret = datum.from_obj_enhance(obj))) {// ret = -1;
    // STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(obj));
  } else if (0 > (ret = append_column(datum))) {// ret = -2;
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "Failed to append datum column", K(ret), K(datum));
    }
  }
  return ret;
}

template<class T>
int KwRowWriter::append(const T &value)
{
  int ret = 0;//OB_SUCCESS;
  if (pos_ + static_cast<int64_t>(sizeof(T)) > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *(reinterpret_cast<T*>(buf_ + pos_)) = value;
    pos_ += sizeof(T);
  }
  return ret;
}

int KwRowWriter::get_uint_byte(const uint64_t uint_value, int64_t &bytes)
{
    int ret = 0;//OB_SUCCESS;
    if (uint_value <= 0xFF) {
      bytes = 1;
    } else if (uint_value <= static_cast<uint16_t>(0xFFFF)){
      bytes = 2;
    } else if (uint_value <= static_cast<uint32_t>(0xFFFFFFFF)) {
      bytes = 4;
    } else {
      bytes = 8;
    }
    return ret;
}