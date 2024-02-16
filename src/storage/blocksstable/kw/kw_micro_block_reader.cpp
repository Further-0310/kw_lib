#include "kw_micro_block_reader.h"

#define USING_LOG_PREFIX COMMON

namespace oceanbase
{
namespace blocksstable
{

template<typename ReaderType>
class PreciseCompare
{
public:
  PreciseCompare(
      int &ret,
      bool &equal,
      ReaderType *reader,
      const char *data_begin,
      const int32_t *index_data,
      const KwTableReadInfo *read_info)
      : ret_(ret),
      equal_(equal),
      reader_(reader),
      data_begin_(data_begin),
      index_data_(index_data),
      read_info_(read_info) {}
  ~PreciseCompare() {}
  inline bool operator() (const int64_t row_idx, const KwDatumRowkey &rowkey)
  {
    return compare(row_idx, rowkey, true);
  }
  inline bool operator() (const KwDatumRowkey &rowkey, const int64_t row_idx)
  {
    return compare(row_idx, rowkey, false);
  }
private:
  inline bool compare(const int64_t row_idx, const KwDatumRowkey &rowkey, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t compare_result = 0;
    if (0 > ret) {
      // do nothing
    } else if (0 > reader_->compare_meta_rowkey(
                rowkey,
                *read_info_,
                data_begin_ + index_data_[row_idx],
                index_data_[row_idx + 1] - index_data_[row_idx],
                compare_result)) { ret = -1;
    //   LOG_WARN("fail to compare rowkey", K(ret), K(rowkey), KPC_(read_info));
    } else {
      bret = lower_bound ? compare_result < 0 : compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal reuslt, must prevent it from being modified again
      if (0 == compare_result && !equal_) {
        equal_ = true;
      }
    }
    return bret;
  }

private:
  int &ret_;
  bool &equal_;
  ReaderType *reader_;
  const char *data_begin_;
  const int32_t *index_data_;
  const KwTableReadInfo *read_info_;
};


/***********************KwIMicroBlockFlatReader*****************************/
KwIMicroBlockFlatReader::KwIMicroBlockFlatReader()
  : header_(nullptr),
    data_begin_(nullptr),
    data_end_(nullptr),
    index_data_(nullptr),
    allocator_(ObModIds::OB_STORE_ROW_GETTER),
    flat_row_reader_()
{
}

KwIMicroBlockFlatReader::~KwIMicroBlockFlatReader()
{
  reset();
}

void KwIMicroBlockFlatReader::reset()
{
  header_ = nullptr;
  data_begin_ = nullptr;
  data_end_ = nullptr;
  index_data_ = nullptr;
  flat_row_reader_.reset();
}

int KwIMicroBlockFlatReader::init(const KwMicroBlockData &block_data)
{
  int ret = 0;//OB_SUCCESS;
  if(OB_UNLIKELY(!block_data.is_valid())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data));
  } else {
    const char *buf = block_data.get_buf();
    header_ = reinterpret_cast<const KwMicroBlockHeader*>(buf);
    data_begin_ = buf + header_->header_size_;
    data_end_ = buf + header_->row_index_offset_;
    index_data_ = reinterpret_cast<const int32_t *>(buf + header_->row_index_offset_);
  }
  return ret;
}

int KwIMicroBlockFlatReader::find_bound_(
    const KwDatumRowkey &key,
    const bool lower_bound,
    const int64_t begin_idx,
    const int64_t end_idx,
    const KwTableReadInfo &read_info,
    int64_t &row_idx,
    bool &equal)
{
  int ret = 0;//OB_SUCCESS;
  equal = false;
  row_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (OB_UNLIKELY(nullptr == data_begin_ || nullptr == index_data_)) {
    ret = -1;//OB_ERR_UNEXPECTED;
    // LOG_WARN("invalid ptr", K(ret), K(data_begin_), K(index_data_));
  } else {
    PreciseCompare<KwRowReader> flat_compare(
        ret,
        equal,
        &flat_row_reader_,
        data_begin_,
        index_data_,
        &read_info);
    KwRowIndexIterator begin_iter(begin_idx);
    KwRowIndexIterator end_iter(end_idx);
    KwRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, flat_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, flat_compare);
    }
    if (0 > ret) {
      LOG_WARN("fail to lower bound rowkey", K(ret), K(key), K(lower_bound), K(read_info));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

/***********************KwMicroBlockGetReader*****************************/
int KwMicroBlockGetReader::inner_init(
    const KwMicroBlockData &block_data,
    const KwTableReadInfo &read_info,
    const KwDatumRowkey &rowkey)
{
  int ret = 0;//OB_SUCCESS;
  if (0 > KwIMicroBlockFlatReader::init(block_data)) { ret = -1;
    LOG_WARN("failed to init reader", K(ret), K(block_data), K(read_info));
  } else {
    row_count_ = header_->row_count_;
    read_info_ = &read_info;
    // if (OB_FAIL(ObIMicroBlockGetReader::init_hash_index(block_data, hash_index_, header_))) {
    //   LOG_WARN("failed to init micro block hash index", K(ret), K(rowkey), K(block_data), K(read_info));
    // } else {
    is_inited_ = true;
    // }
  }
  return ret;
}

int KwMicroBlockGetReader::get_row(
    const KwMicroBlockData &block_data,
    const KwDatumRowkey &rowkey,
    const KwTableReadInfo &read_info,
    KwDatumRow &row)
{
  int ret = 0;
  int64_t row_idx;
  if (!read_info.is_valid()) { ret = -1;
    // ret = OB_INVALID_ARGUMENT;
    // LOG_WARN("Invalid columns info ", K(ret), K(read_info));
  } else if (0 > inner_init(block_data, read_info, rowkey)) { ret = -2;
    // LOG_WARN("fail to inner init ", K(ret), K(block_data));
  } else if (0 > locate_rowkey(rowkey, row_idx)){ ret = -3;
    // if (OB_BEYOND_THE_RANGE != ret) {
    //   LOG_WARN("failed to locate row, ", K(ret), K(rowkey));
    // }
  } else if (0 > flat_row_reader_.read_row(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              &read_info,
              row)) { ret = -4;
    // LOG_WARN("Fail to read row, ", K(ret), K(rowkey));
  } else {
    row.fast_filter_skipped_ = false;
  }
  return ret;
}

int KwMicroBlockGetReader::locate_rowkey(const KwDatumRowkey &rowkey, int64_t &row_idx)
{
  int ret = 0;//OB_SUCCESS;
  bool need_binary_search = false;
  bool found = false;
  if (IS_NOT_INIT) { ret = -1;
    // ret = OB_NOT_INIT;
    // LOG_WARN("not init", K(ret));
  } else if (0 > locate_rowkey_fast_path(rowkey, row_idx, need_binary_search, found)) { ret = -2;
    // LOG_WARN("faile to locate rowkey by hash index", K(ret));
  } else if (need_binary_search) {
    bool is_equal = false;
    if (0 > KwIMicroBlockFlatReader::find_bound_(rowkey, true/*lower_bound*/, 0, row_count_,
        *read_info_, row_idx, is_equal)) { ret = -3;
    //   LOG_WARN("fail to lower_bound rowkey", K(ret));
    } else if (row_count_ == row_idx || !is_equal) {
      row_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      ret = -4;//OB_BEYOND_THE_RANGE;
    } else {
      const KwRowHeader *row_header =
          reinterpret_cast<const KwRowHeader*>(data_begin_ + index_data_[row_idx]);
      if (row_header->get_row_multi_version_flag().is_ghost_row()) {
        row_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
        ret = -5;//OB_BEYOND_THE_RANGE;
      }
    }
  } else if (!found) {
    ret = -6;//OB_BEYOND_THE_RANGE;
  }
  return ret;
}

// 未实现hash_index相关逻辑
int KwMicroBlockGetReader::locate_rowkey_fast_path(const KwDatumRowkey &rowkey,
                                                   int64_t &row_idx,
                                                   bool &need_binary_search,
                                                   bool &found)
{
  int ret = 0;//OB_SUCCESS;
//   need_binary_search = false;
//   if (hash_index_.is_inited()) {
//     uint64_t hash_value = 0;
//     const blocksstable::ObStorageDatumUtils &datum_utils = read_info_->get_datum_utils();
//     if (OB_FAIL(rowkey.murmurhash(0, datum_utils, hash_value))) {
//       LOG_WARN("Failed to calc rowkey hash", K(ret), K(rowkey), K(datum_utils));
//     } else  {
//       const uint8_t tmp_row_idx = hash_index_.find(hash_value);
//       if (tmp_row_idx == ObMicroBlockHashIndex::NO_ENTRY) {
//         row_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
//         found = false;
//       } else if (tmp_row_idx == ObMicroBlockHashIndex::COLLISION) {
//         need_binary_search = true;
//       } else {
//         int32_t compare_result = 0;
//         if (OB_UNLIKELY(tmp_row_idx >= row_count_)) {
//           ret = OB_ERR_UNEXPECTED;
//           LOG_WARN("Unexpected row_idx", K(ret), K(tmp_row_idx), K(row_count_), K(rowkey), KPC_(read_info));
//         } else if (OB_FAIL(flat_row_reader_.compare_meta_rowkey(
//                       rowkey,
//                       *read_info_,
//                       data_begin_ + index_data_[tmp_row_idx],
//                       index_data_[tmp_row_idx + 1] - index_data_[tmp_row_idx],
//                       compare_result))) {
//           LOG_WARN("fail to compare rowkey", K(ret), K(rowkey), KPC_(read_info));
//         } else if (0 != compare_result) {
//           row_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
//           found = false;
//         } else {
//           row_idx = tmp_row_idx;
//           found = true;
//         }
//       }
//     }
//   } else {
    need_binary_search = true;
//   }
  return ret;
}

/***********************KwMicroBlockReader*****************************/
void KwMicroBlockReader::reset()
{
  KwIMicroBlockFlatReader::reset();
  KwIMicroBlockReader::reset();
  header_ = NULL;
  data_begin_ = NULL;
  data_end_ = NULL;
  index_data_ = NULL;
  flat_row_reader_.reset();
  allocator_.reuse();
}

int KwMicroBlockReader::init(
    const KwMicroBlockData &block_data,
    const KwTableReadInfo &read_info)
{
  int ret = 0;//OB_SUCCESS;
  if (IS_INIT) {
    reset();
  }
  if (/*OB_UNLIKELY*/(!read_info.is_valid())) { //ret = -1;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("columns info is invalid", K(ret), K(read_info));
  } else if (0 > (ret = KwIMicroBlockFlatReader::init(block_data))) {
    // LOG_WARN("fail to init, ", K(ret));
  } else {
    row_count_ = header_->row_count_;
    read_info_ = &read_info;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int KwMicroBlockReader::find_bound(
    const KwDatumRowkey &key,
    const bool lower_bound,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal)
{
  int ret = 0;//OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = -1;//OB_NOT_INIT;
    // LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < 0 || nullptr == read_info_)) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), K(key), K(begin_idx), K(row_count_),
    //          KP_(data_begin), KP_(index_data), KP_(read_info));
  } else if (0 > KwIMicroBlockFlatReader::find_bound_(
          key,
          lower_bound,
          begin_idx,
          row_count_,
          *read_info_,
          row_idx,
          equal)) { ret = -3;
    // LOG_WARN("failed to find bound", K(ret), K(lower_bound), K(begin_idx), K_(row_count), KPC_(read_info));
  }
  return ret;
}

int KwMicroBlockReader::find_bound(
    const KwDatumRange &range,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal,
    int64_t &end_key_begin_idx,
    int64_t &end_key_end_idx)
{
  UNUSEDx(end_key_begin_idx, end_key_end_idx);
  return find_bound(range.get_start_key(), true, begin_idx, row_idx, equal);
}

int KwMicroBlockReader::get_row(const int64_t index, KwDatumRow &row)
{
  int ret = 0;
  if(!is_inited_){ ret = -13;
    // ret = OB_NOT_INIT;
    // LOG_WARN("should init reader first, ", K(ret));
  } else if(nullptr == header_ ||
            nullptr == read_info_ ||
            index < 0 || index >= header_->row_count_ ||
            !row.is_valid()) { ret = -2;
    // ret = OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), K(index), K(row), KPC_(header), KPC_(read_info));
  } else if (0 > (ret = flat_row_reader_.read_row(
              data_begin_ + index_data_[index],
              index_data_[index + 1] - index_data_[index],
              read_info_,
              row))) { //ret = -3;
    LOG_WARN("row reader read row failed", K(ret), K(index), K(index_data_[index + 1]),
             K(index_data_[index]), /*KPC_(header),*/ KPC_(read_info));
  } else {
    row.fast_filter_skipped_ = false;
    // LOG_DEBUG("row reader read row success", K(ret), KPC_(read_info), K(index), K(index_data_[index + 1]),
    //         K(index_data_[index]), K(row));
  }
  return ret;
}

int KwMicroBlockReader::get_row_header(const int64_t row_idx, const KwRowHeader *&row_header)
{
    int ret = 0;
    if(IS_NOT_INIT) { ret = -1; } // warn "reader not init"
    else if(nullptr == header_ || row_idx >= header_->row_count_) { ret = -2; } // warn "Id is NULL"
    else if(0 > flat_row_reader_.read_row_header(
        data_begin_ + index_data_[row_idx],
        index_data_[row_idx + 1] - index_data_[row_idx],
        row_header)) { ret = -3; } // warn "failed to setup row"
    return ret;
}

int KwMicroBlockReader::get_row_count(int64_t &row_count)
{
  int ret = 0;//OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = -1;//OB_NOT_INIT;
    // LOG_WARN("not init", K(ret));
  } else {
    row_count = header_->row_count_;
  }
  return ret;
}

int KwMicroBlockReader::get_row_count(
    int32_t col,
    const int64_t *row_ids,
    const int64_t row_cap,
    const bool contains_null,
    int64_t &count)
{
  int ret = 0;//OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ ||
                  nullptr == read_info_ ||
                  row_cap > header_->row_count_)) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // LOG_WARN("Invalid argument", K(ret), KPC(header_), KPC_(read_info), K(row_cap), K(col));
  } else if (contains_null) {
    count = row_cap;
  } else {
    count = 0;
    int64_t row_idx = common::OB_INVALID_INDEX;
    const common::ObIArray<int32_t> &cols_index = read_info_->get_columns_index();
    // const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    int64_t col_idx = cols_index.at(col);
    KwStorageDatum datum;
    for (int64_t i = 0; 0 == ret && i < row_cap; ++i) {
      row_idx = row_ids[i];
      if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
        ret = -2;//OB_ERR_UNEXPECTED;
        // LOG_WARN("Uexpected row idx", K(ret), K(row_idx), KPC(header_));
      } else if (0 > flat_row_reader_.read_column(
          data_begin_ + index_data_[row_idx],
          index_data_[row_idx + 1] - index_data_[row_idx],
          col_idx,
          datum)) { ret = -3;
        // LOG_WARN("fail to read column", K(ret), K(i), K(col_idx), K(row_idx));
      } else if (!datum.is_null()) {
        ++count;
      }
    }
  }
  return ret;
}

// notice, trans_version of ghost row is min(0)
int KwMicroBlockReader::get_multi_version_info(
    const int64_t row_idx,
    const int64_t schema_rowkey_cnt,
    const KwRowHeader *&row_header,
    int64_t &trans_version,
    int64_t &sql_sequence)
{
  int ret = 0;//OB_SUCCESS;
  row_header = nullptr;
  if (IS_NOT_INIT) {
    ret = -1;//OB_NOT_INIT;
    // LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == header_ ||
                         row_idx < 0 || row_idx > row_count_ ||
                         0 > schema_rowkey_cnt ||
                         header_->column_count_ < schema_rowkey_cnt + 2)) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument", K(ret), K(row_idx), K_(row_count), K(schema_rowkey_cnt),
    //          KPC_(header), K(lbt()));
  } else if (0 > flat_row_reader_.read_row_header(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              row_header)) { ret = -3;
    // LOG_WARN("fail to setup row", K(ret), K(row_idx), K(index_data_[row_idx + 1]),
    //          K(index_data_[row_idx]), KP(data_begin_));
  } else {
    KwStorageDatum datum;
    const int64_t read_col_idx =
      row_header->get_row_multi_version_flag().is_uncommitted_row()
      ? schema_rowkey_cnt + 1 : schema_rowkey_cnt;
    if (0 > flat_row_reader_.read_column(
                data_begin_ + index_data_[row_idx],
                index_data_[row_idx + 1] - index_data_[row_idx],
                read_col_idx,
                datum)) { ret = -4;
    //   LOG_WARN("fail to read column", K(ret), K(read_col_idx));
    } else {
      if (!row_header->get_row_multi_version_flag().is_uncommitted_row()) {
        // get trans_version for committed row
        sql_sequence = 0;
        trans_version = row_header->get_row_multi_version_flag().is_ghost_row() ? 0 : -datum.get_int();
      } else {
        // get sql_sequence for uncommitted row
        trans_version = INT64_MAX;
        sql_sequence = -datum.get_int();
      }
    }
  }

  return ret;
}

int KwMicroBlockReader::get_rows(
    const common::ObIArray<int32_t> &cols_projector,
    const common::ObIArray<const ObColumnParam *> &col_params,
    const common::ObIArray<ObObjDatumMapType> &map_types, // TODO remove this, use datums directly
    const KwDatumRow &default_row,
    const int64_t *row_ids,
    const int64_t row_cap,
    KwDatumRow &row_buf,
    common::ObIArray<ObDatum *> &datums)//,
    // sql::ExprFixedArray &exprs,
    // sql::ObEvalCtx &eval_ctx)
{
    int ret = 0;
    int64_t row_idx = -1; //common::OB_INVALID_INDEX;
    allocator_.reuse();
    if(nullptr == header_ || nullptr == read_info_ || row_cap > header_->row_count_ || !row_buf.is_valid()) { ret = -1; } // "Invalid argument"
    else if(0 > row_buf.reserve(read_info_->get_request_count())) { ret = -2; } // warn "Failed to reserve row buf"
    else {
        for(int64_t idx = 0; 0 == ret && idx < row_cap; ++idx) { 
            row_idx = row_ids[idx];
            if(row_idx < 0 || row_idx >= header_->row_count_) { ret = -3; } // warn "Unexpected row idx"
            else if(0 > flat_row_reader_.read_row(
                data_begin_ + index_data_[row_idx],
                index_data_[row_idx + 1] - index_data_[row_idx],
                read_info_,
                row_buf)) { ret = -4; } // warn "Fail to read row"
            else {
                for(int64_t i = 0; 0 == ret && i < cols_projector.count(); i++) {
                    // KwDatum &datum = datums.at(i)[idx];
                    common::ObDatum &datum = datums.at(i)[idx];
                    int32_t col_idx = cols_projector.at(i);
                    if (col_idx >= read_info_->get_request_count()) { ret = -5; } // warn "Unexpected col idx"
                    // else if (row_buf.datums_[col_idx].is_nop()){
                    else if (row_buf.storage_datums_[col_idx].is_nop()){
                    //   if(default_row.datums_[i].is_nop()) {} // virtual columns will be calculated in sql
                      if(default_row.storage_datums_[i].is_nop()) {} // virtual columns will be calculated in sql
                    //   else if (0 > datum.from_storage_datum(default_row.datums_[i], map_types.at(i))) { ret = -6; } // fill columns added // warn "Fail to transfer datum"
                      else if (0 > datum.from_storage_datum(default_row.storage_datums_[i], map_types.at(i))) { ret = -6; } // fill columns added // warn "Fail to transfer datum"
                    }
                    else{
                      bool need_copy = false;
                    //   if(row_buf.datums_[col_idx].need_copy_for_encoding_column_with_flat_format(map_types.at(i))){
                      // if(row_buf.storage_datums_[col_idx].need_copy_for_encoding_column_with_flat_format(map_types.at(i))){
                      //   exprs[i]->reset_ptr_in_datum(eval_ctx, idx);
                      //   need_copy = true;
                      // }
                    //   if(0 > datum.from_storage_datum(row_buf.datums_[col_idx], map_types.at(i), need_copy)) { ret = -7; } // warn "Failed to from storage datum"
                      if(0 > datum.from_storage_datum(row_buf.storage_datums_[col_idx], map_types.at(i), need_copy)) { ret = -7; } // warn "Failed to from storage datum"
                    }
                }
            }
        }

        // 没有实现完毕
        // if(0 == ret) {
        //   for(int64_t i = 0; 0 == ret && i < cols_projector.count(); ++i){
        //     if(nullptr != col_params.at(i)){
        //       if(0 > storage::pad_on_datums(
        //         col_params.at(i)->get_accuracy(),
        //         col_params.at(i)->get_meta_type().get_collation_type(),
        //         allocator_,
        //         row_cap,
        //         datums.at(i))) 
        //       { ret = -8; } // warn "fail to pad on datums"
        //     }
        //   }
        // }
    }
    return ret;
}

/***********ob_storage_utils.h：pad_on_datums()************/
// int pad_on_datums(const common::ObAccuracy accuracy,
//                   const common::ObCollationType cs_type,
//                   common::ObIAllocator &padding_alloc,
//                   int64_t row_count,
//                   KwDatum *&datums)
// {
//   int ret = 0;
//   ObLength length = accuracy.get_length(); // byte or char length
//   const ObString space_pattern = get_padding_str(cs_type);
//   bool is_oracle_byte = is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics());
//   char *buf = nullptr;
//   if(1 == length){
//     int32_t buf_len = space_pattern.length();
//     if(NULL == (buf = (char*) padding_alloc.alloc(buf_len))) { ret = -1; } // OB_ALLOCATE_MEMORY_FAILED, warn "no memory"
//     else{
//       append_padding_pattern(space_pattern, 0, buf_len, buf);
//       for(int64_t i = 0; i < row_count; i++){
//         KwDatum &datum = datums[i];
//         if(datum.is_null()){} // do nothing
//         else if(0 == datum.pack_){
//           datum.ptr_ = buf;
//           datum.pack_ = buf_len;
//         }
//       }
//     }
//   }
//   else if(can_do_ascii_optimize(cs_type)) {
//     int32_t buf_len = length * space_pattern.length() * row_count;
//     if(NULL == (buf = (char*) padding_alloc.alloc(buf_len))) { ret = -1; } // OB_ALLOCATE_MEMORY_FAILED, warn "no memory"
//     else{
//       char *ptr = buf;
//       memset(buf, OB_PADDING_CHAR, buf_len);
//       for(int64_t i = 0; 0 == ret && i < row_count; i++){
//         KwDatum &datum = datums[i];
//         if(datum.is_null()) {} // do nothing
//         else{
//           if(is_oracle_byte || is_ascii_str(datum.ptr_, datum.pack_)){
//             if(datum.pack_ < length){
//               memcpy(ptr, datum.ptr_, datum.pack_);
//               datum.ptr_ = ptr;
//               datum.pack_ = length;
//               ptr += length;
//             }
//           }
//           else{
//             int32_t cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_));
//             if(cur_len < length && 0 > pad_datum_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum)) { ret = -1; }
//             // STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
//           }
//         }
//       }
//     }
//   }
//   else {
//     for(int64_t i = 0; 0 == ret && i < row_count; i++){
//       KwDatum &datum = datums[i];
//       if(datum.is_null()){} // do nothing
//       else{
//         int32_t cur_len = 0;
//         if(is_oracle_byte) { cur_len = datum.pack_; }
//         else{ cur_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, datum.ptr_, datum.pack_)); }
//         if(cur_len < length && 0 > pad_datum_on_local_buf(space_pattern, length - cur_len, padding_alloc, datum)) { ret = -1; }
//         // STORAGE_LOG(WARN, "fail to pad on padding allocator", K(ret), K(length), K(cur_len), K(datum));
//       }
//     }
//   }
//   return ret;
// }

// static const ObString KW_DEFAULT_APDDING_STRING(1, 1, &OB_PADDING_CHAR);

// inline static const ObString get_padding_str(ObCollationType coll_type)
// {
//   if(!ObCharset::is_cs_nonascii(coll_type)) { return KW_DEFAULT_APDDING_STRING; }
//   else { return ObCharsetUtils::get_const_str(coll_type, OB_PADDING_CHAR); }
  
// }

// inline static void append_padding_pattern(const ObString &space_pattern,
//                                           const int32_t offset,
//                                           const int32_t buf_len,
//                                           char *&buf)
// {
//   if(1 == space_pattern.length()) { memset(buf + offset, space_pattern[0], buf_len - offset); }
//   else{
//     for(int32_t i = offset; i < buf_len; i+= space_pattern.length()){
//       memcpy(buf + i, space_pattern.ptr(), space_pattern.length());
//     }
//   }
// }

// inline static int pad_datum_on_local_buf(const ObString &space_pattern,
//                                          int32_t pad_whitespace_length,
//                                          common::ObIAllocator &padding_alloc,
//                                          KwDatum &datum)
// {
//   int ret = 0;
//   char *buf = nullptr;
//   const int32_t buf_len = datum.pack_ + pad_whitespace_length * space_pattern.length();
//   if(NULL == (buf = (char*) padding_alloc.alloc(buf_len))){ ret = -1; } // OB_ALLOCATE_MEMORY_FAILED, warn "no memory"
//   else{
//     memcpy(buf, datum.ptr_, datum.pack_);
//     append_padding_pattern(space_pattern, datum.pack_, buf_len, buf);
//     datum.ptr_ = buf;
//     datum.pack_ = buf_len;
//   }
//   return ret;
// }

} // namespace blocksstable
} // namespace oceanbase
