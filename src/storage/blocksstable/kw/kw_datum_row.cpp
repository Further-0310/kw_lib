/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "kw_datum_row.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
static int nonext_nonext_compare(const KwStorageDatum &left, const KwStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  cmp_ret = cmp_func.cmp_func_(left, right, cmp_ret);
  // STORAGE_LOG(DEBUG, "chaser debug compare datum", K(left), K(right), K(cmp_ret));
  return 0;//OB_SUCCESS;
}

static int nonext_ext_compare(const KwStorageDatum &left, const KwStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = 0;//OB_SUCCESS;
  UNUSEDx(left, cmp_func);
  if (right.is_max()) {
    cmp_ret = -1;
  } else if (right.is_min()) {
    cmp_ret = 1;
  } else {
    ret = -1;//OB_ERR_SYS;
    // STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(right));
  }
  // STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));
  return ret;
}

static int ext_nonext_compare(const KwStorageDatum &left, const KwStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = 0;//OB_SUCCESS;
  UNUSEDx(right, cmp_func);
  if (left.is_max()) {
    cmp_ret = 1;
  } else if (left.is_min()) {
    cmp_ret = -1;
  } else {
    ret = -1;//OB_ERR_SYS;
    // STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(left));
  }
  // STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));
  return ret;
}

static int ext_ext_compare(const KwStorageDatum &left, const KwStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = 0;//OB_SUCCESS;
  UNUSEDx(cmp_func);
  int64_t lv = left.is_max() - left.is_min();
  int64_t rv = right.is_max() - right.is_min();
  if (OB_UNLIKELY(0 == lv || 0 == rv)) {
    ret = -1;//OB_ERR_SYS;
    // STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(left), K(right));
  } else {
    cmp_ret = lv - rv;
  }
  // STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));

  return ret;
}

typedef int (*ExtSafeCompareFunc)(const KwStorageDatum &left, const KwStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret);
static ExtSafeCompareFunc ext_safe_cmp_funcs[2][2] = {
  {nonext_nonext_compare, nonext_ext_compare},
  {ext_nonext_compare, ext_ext_compare}
};

int KwStorageDatumCmpFunc::compare(const KwStorageDatum &left, const KwStorageDatum &right, int &cmp_ret) const
{
  return ext_safe_cmp_funcs[left.is_ext()][right.is_ext()](left, right, cmp_func_, cmp_ret);
}


const char *kw_get_dml_str(KwDmlFlag dml_flag)
{
  STATIC_ASSERT(static_cast<int64_t>(KW_DF_MAX) == ARRAYSIZEOF(KwDmlFlagStr), "dml str len is mismatch");
  const char *ret_str = nullptr;
  if (dml_flag >= KW_DF_MAX) {
    ret_str = "invalid_dml";
  } else {
    ret_str = KwDmlFlagStr[dml_flag];
  }
  return ret_str;
}

void kw_format_dml_str(const int32_t flag, char *str, int len) {
  assert(len >= 16);
  int32_t bit;
  int32_t count = 0;
  uint32_t f = flag;
  memset(str, 0, len);
  if (f & (1<<7)) {
    strncat(str, KwDmlTypeStr[1], 16 - strlen(str));
  } else {
    strncat(str, KwDmlTypeStr[0], 16 - strlen(str));
  }
  f = f & ((1 << 7) - 1);
  strncat(str, "|", 16 - strlen(str));
  strncat(str, KwDmlTypeStr[f], 16 - strlen(str));
}

// OB_SERIALIZE_MEMBER(KwDmlRowFlag, whole_flag_);

// OB_SERIALIZE_MEMBER(KwMultiVersionRowFlag, flag_);

/*
 *KwStorageDatumBuffer
 */
KwStorageDatumBuffer::KwStorageDatumBuffer(common::ObIAllocator *allocator)
    : capacity_(LOCAL_BUFFER_ARRAY),
      local_datums_(),
      datums_(local_datums_),
      allocator_(allocator),
      is_inited_(nullptr != allocator)
{}

void KwStorageDatumBuffer::reset()
{
  if (datums_ != local_datums_ && nullptr != allocator_) {
    allocator_->free(datums_);
  }
  allocator_ = nullptr;
  datums_ = local_datums_;
  capacity_ = LOCAL_BUFFER_ARRAY;
  for (int64_t i = 0; i < capacity_; i++) {
    datums_[i].reuse();
  }
  is_inited_ = false;
}

int KwStorageDatumBuffer::init(common::ObIAllocator &allocator)
{
  int ret = 0;//OB_SUCCESS;

  if (IS_INIT) {
    ret = -1;//OB_INIT_TWICE;
    // STORAGE_LOG(WARN, "KwStorageDatumBuffer init twice", K(ret), K(*this));
  } else {
    /*OB_ASSERT*/assert(datums_ == local_datums_);
    allocator_ = &allocator;
    is_inited_ = true;
  }

  return ret;
}

int KwStorageDatumBuffer::reserve(const int64_t count, const bool keep_data)
{
  int ret = 0;//OB_SUCCESS;
  void *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = -1;//OB_NOT_INIT;
    // STORAGE_LOG(WARN, "KwStorageDatumBuffer is not inited", K(ret), K(*this));
  } else if (/*OB_UNLIKELY*/(count <= 0)) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to reserve datum buffer", K(ret), K(count));
  } else if (count <= capacity_){
  } else if (/*OB_ISNULL*/ NULL == (buf = allocator_->alloc(sizeof(KwStorageDatum) * count))) {
      ret = -3;//OB_ALLOCATE_MEMORY_FAILED;
      // STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(count));
  } else {
    KwStorageDatum *new_datums = new (buf) KwStorageDatum [count];
    if (keep_data) {
      for (int64_t i = 0; i < capacity_; i++) {
        new_datums[i] = datums_[i];
      }
    }
    if (nullptr != datums_ && datums_ != local_datums_) {
      allocator_->free(datums_);
    }
    datums_ = new_datums;
    capacity_  = count;
  }

  return ret;
}

/*
 *KwDatumRow
 */

KwDatumRow::KwDatumRow()
  : local_allocator_("KwDatumRow"),
    count_(0),
    fast_filter_skipped_(false),
    have_uncommited_row_(false),
    row_flag_(),
    mvcc_row_flag_(),
    // trans_id_(),
    trans_id_(0),
    scan_index_(0),
    group_idx_(0),
    snapshot_version_(0),
    storage_datums_(nullptr),
    datum_buffer_()//,
    // old_row_(),
    // obj_buf_()
{}

KwDatumRow::~KwDatumRow()
{
  reset();
}

int KwDatumRow::init(ObIAllocator &allocator, const int64_t capacity)
{
  int ret = 0;//OB_SUCCESS;
  if (/*OB_UNLIKELY*/(is_valid())) {
    ret = -1;//OB_INIT_TWICE;
    // STORAGE_LOG(WARN, "KwDatumRow init twice", K(ret), K(*this));
  // } else if (/*OB_FAIL*/ 0 > ( ret = obj_buf_.init(&allocator))) {
    // STORAGE_LOG(WARN, "Failed to init obj_buf array", K(ret));
  } else if (/*OB_FAIL*/ 0 > ( ret = datum_buffer_.init(allocator))) {
    // STORAGE_LOG(WARN, "Failed to init datum buffer", K(ret));
  } else if (/*OB_FAIL*/ 0 > ( ret = datum_buffer_.reserve(capacity))) {
    // STORAGE_LOG(WARN, "Failed to reserve datum buffer", K(ret), K(capacity));
  } else {
    storage_datums_ = datum_buffer_.get_datums();
    count_ = capacity;
  }

  return ret;
}
int KwDatumRow::init(const int64_t capacity)
{
  int ret = 0;//OB_SUCCESS;

  if (/*OB_UNLIKELY*/(is_valid())) {
    ret = -1;//OB_INIT_TWICE;
    // STORAGE_LOG(WARN, "KwDatumRow init twice", K(ret), K(*this));
  } else if (/*OB_UNLIKELY*/(capacity <= 0 || capacity > 2 * OB_USER_ROW_MAX_COLUMNS_COUNT)) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to init datumrow", K(ret), K(capacity));
  } else {
    ret = init(local_allocator_, capacity);
  }

  return ret;
}

//// only for transformer which never use old_row_
//int KwDatumRow::init_buf(char *buf, const int64_t buf_size, const int64_t capacity)
//{
  //int ret = OB_SUCCESS;

  //if (OB_UNLIKELY(is_valid())) {
    //ret = OB_INIT_TWICE;
    //STORAGE_LOG(WARN, "KwDatumRow init twice", K(ret), K(*this));
  //} else if (OB_UNLIKELY(nullptr == buf || capacity <= 0 || buf_size < capacity * sizeof(KwStorageDatum))) {
    //ret = OB_INVALID_ARGUMENT;
    //STORAGE_LOG(WARN, "Invalid argument to init datum row", K(ret), KP(buf), K(buf_size), K(capacity));
  //} else {
    //storage_datums_ = new (buf) KwStorageDatum [capacity];
    //count_ = capacity_ = capacity;
    //allocator_ = nullptr;
    //old_row_.reset();
  //}

  //return ret;
//}

int KwDatumRow::reserve(const int64_t capacity, const bool keep_data)
{
  int ret = 0;//OB_SUCCESS;
  void *buf = nullptr;

  if (/*OB_UNLIKELY*/(!is_valid())) {
    // STORAGE_LOG(WARN, "KwDatumRow is not inited", K(ret), K(*this));
  } else if (/*OB_UNLIKELY*/(capacity <= 0 || capacity > 2 * OB_USER_ROW_MAX_COLUMNS_COUNT)) {
    // STORAGE_LOG(WARN, "Invalid argument to reserve datum row", K(ret), K(capacity));
  } else if (capacity <= get_capacity()) {
    // skip
  } else if (/*OB_FAIL*/ 0 > (ret = datum_buffer_.reserve(capacity, keep_data))) {
    // STORAGE_LOG(WARN, "Failed to reserve datum buffer", K(ret), K(capacity));
  // } else if (/*OB_FAIL*/ 0 > (ret = obj_buf_.reserve(capacity))) {
    // STORAGE_LOG(WARN, "Failed to reserve obj buf", K(ret), K(capacity));
  } else {
    storage_datums_ = datum_buffer_.get_datums();
    // old_row_.reset();
  }
  if (/*OB_SUCC*/0 == (ret)) {
    mvcc_row_flag_.reset();
    // trans_id_.reset();
    trans_id_ = 0;
    scan_index_ = 0;
    group_idx_ = 0;
    snapshot_version_ = 0;
    fast_filter_skipped_ = false;
    have_uncommited_row_ = false;
  }

  return ret;
}


void KwDatumRow::reset()
{
  // old_row_.reset();
  // obj_buf_.reset();
  fast_filter_skipped_ = false;
  datum_buffer_.reset();
  storage_datums_ = nullptr;
  snapshot_version_ = 0;
  group_idx_ = 0;
  scan_index_ = 0;
  // trans_id_.reset();
  trans_id_ = 0;
  mvcc_row_flag_.reset();
  row_flag_.reset();
  count_ = 0;
  local_allocator_.reset();
}

void KwDatumRow::reuse()
{
  if (nullptr != storage_datums_) {
    for (int64_t i = 0; i < count_; i++) {
      storage_datums_[i].reuse();
    }
  }
  mvcc_row_flag_.reset();
  // trans_id_.reset();
  trans_id_ = 0;
  scan_index_ = 0;
  group_idx_ = 0;
  snapshot_version_ = 0;
  fast_filter_skipped_ = false;
  have_uncommited_row_ = false;
}

int KwDatumRow::deep_copy(const KwDatumRow &src, ObIAllocator &allocator)
{
  int ret = 0;//OB_SUCCESS;

  if (/*OB_UNLIKELY*/(!src.is_valid())) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to deep copy datum row", K(ret), K(src));
  } else if (/*OB_UNLIKELY*/(get_capacity() < src.count_ || nullptr == storage_datums_)) {
    ret = -2;//OB_ERR_UNEXPECTED;
    // STORAGE_LOG(WARN, "Unexpected local datum row to deep copy", K(ret), KPC(this));
  } else {
    count_ = src.count_;
    row_flag_ = src.row_flag_;
    mvcc_row_flag_  = src.mvcc_row_flag_;
    trans_id_ = src.trans_id_;
    scan_index_ = src.scan_index_;
    group_idx_ = src.group_idx_;
    snapshot_version_ = src.snapshot_version_;
    fast_filter_skipped_ = src.fast_filter_skipped_;
    have_uncommited_row_ = src.have_uncommited_row_;
    for(int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
      if (/*OB_FAIL*/ 0 > (ret = storage_datums_[i].deep_copy(src.storage_datums_[i], allocator))) {
        // STORAGE_LOG(WARN, "Failed to deep copy storage datum", K(ret), K(src.storage_datums_[i]));
      }
    }
  }

  return ret;
}

// int KwDatumRow::prepare_new_row(const ObIArray<share::schema::ObColDesc> &out_cols)
// {
//   int ret = 0;//OB_SUCCESS;

//   if (/*OB_UNLIKELY*/(!is_valid())) {
//     ret = -1;//OB_NOT_INIT;
//     // STORAGE_LOG(WARN, "KwDatumRow is not inited", K(ret), K(*this));
//   } else if (/*OB_UNLIKELY*/(out_cols.count() < count_)) {
//     ret = -2;//OB_INVALID_ARGUMENT;
//     // STORAGE_LOG(WARN, "Invalid argument to prepare new row", K(ret), K(count_), K(out_cols));
//   } else if (/*OB_FAIL*/ 0 > (ret = obj_buf_.reserve(count_))) {
//     // STORAGE_LOG(WARN, "Failed to reserve buf for obj buf", K(ret), K(count_));
//   } else {
//     old_row_.cells_ = obj_buf_.get_data();
//     old_row_.count_ = count_;
//     old_row_.projector_ = nullptr;
//     old_row_.projector_size_ = 0;
//     for (int64_t i = 0; OB_SUCC(ret) && i < out_cols.count(); i++) {
//       if (/*OB_FAIL*/ 0 > (ret = storage_datums_[i].to_obj_enhance(old_row_.cells_[i], out_cols.at(i).col_type_))) {
//         // STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret), K(i), K(storage_datums_[i]));
//       }
//     }
//   }

//   return ret;
// }

// int KwDatumRow::to_store_row(const ObIArray<share::schema::ObColDesc> &out_cols,
//                                    storage::ObStoreRow &store_row)
// {
//   int ret = 0;//OB_SUCCESS;

//   if (/*OB_UNLIKELY*/(!is_valid())) {
//     // STORAGE_LOG(WARN, "KwDatumRow is not inited", K(ret), K(*this));
//   } else if (/*OB_FAIL*/ 0 > (ret = prepare_new_row(out_cols))) {
//     // STORAGE_LOG(WARN, "Failed to prepare new row", K(ret));
//   } else {
//     store_row.reset();
//     store_row.row_val_ = old_row_;
//     store_row.capacity_ = count_;
//     // store_row.flag_ = row_flag_;
//     // store_row.flag_ = ObDmlRowFlag(row_flag_.whole_flag_);
//     store_row.flag_ = ObDmlRowFlag(row_flag_.get_serialize_flag());
//     // store_row.row_type_flag_ = mvcc_row_flag_;
//     store_row.row_type_flag_ = ObMultiVersionRowFlag(mvcc_row_flag_.flag_); 
//     store_row.trans_id_ = trans_id_;
//     store_row.scan_index_ = scan_index_;
//     store_row.group_idx_ = group_idx_;
//     store_row.snapshot_version_ = snapshot_version_;
//     store_row.fast_filter_skipped_ = fast_filter_skipped_;
//   }

//   return ret;
// }

// int KwDatumRow::from_store_row(const storage::ObStoreRow &store_row)
// {
//   int ret = 0;//OB_SUCCESS;

//   if (/*OB_UNLIKELY*/(!is_valid())) {
//     // STORAGE_LOG(WARN, "KwDatumRow is not inited", K(ret), K(*this));
//   } else if (/*OB_FAIL*/ 0 > (ret = reserve(store_row.row_val_.count_))) {
//     // STORAGE_LOG(WARN, "Failed to reserve datums", K(ret), K(store_row));
//   } else {
//     count_ = store_row.row_val_.count_;
//     // row_flag_ = store_row.flag_;
//     row_flag_ = KwDmlRowFlag(store_row.flag_.get_serialize_flag());
//     // mvcc_row_flag_ = store_row.row_type_flag_;
//     mvcc_row_flag_ = KwMultiVersionRowFlag(store_row.row_type_flag_.flag_);
//     trans_id_ = store_row.trans_id_;
//     scan_index_ = store_row.scan_index_;
//     group_idx_ = store_row.group_idx_;
//     snapshot_version_ = store_row.snapshot_version_;
//     fast_filter_skipped_ = store_row.fast_filter_skipped_;
//     have_uncommited_row_ = false;
//     for (int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
//       if (/*OB_FAIL*/ 0 > (ret = storage_datums_[i].from_obj_enhance(store_row.row_val_.cells_[i]))) {
//         // STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(i), K(store_row.row_val_.cells_[i]));
//       }
//     }
//   }

//   return ret;
// }

// OB_DEF_SERIALIZE(KwDatumRow)
// {
//   int ret = OB_SUCCESS;
//   LST_DO_CODE(OB_UNIS_ENCODE,
//               row_flag_,
//               mvcc_row_flag_,
//               trans_id_,
//               scan_index_,
//               group_idx_,
//               snapshot_version_);
//   OB_UNIS_ENCODE_ARRAY(storage_datums_, count_);
//   return ret;
// }

// OB_DEF_DESERIALIZE(KwDatumRow)
// {
//   int ret = OB_SUCCESS;
//   fast_filter_skipped_ = false;
//   have_uncommited_row_ = false;
//   LST_DO_CODE(OB_UNIS_DECODE,
//               row_flag_,
//               mvcc_row_flag_,
//               trans_id_,
//               scan_index_,
//               group_idx_,
//               snapshot_version_);
//   OB_UNIS_DECODE(count_);
//   if (OB_FAIL(ret)) {
//   } else if (get_capacity() < count_) {
//     ret = OB_ERR_UNEXPECTED;
//     STORAGE_LOG(WARN, "KwDatumRow has not keep enough datums for deserialize", K(ret), K_(datum_buffer), K_(count));
//   } else {
//     OB_UNIS_DECODE_ARRAY(storage_datums_, count_);
//     fast_filter_skipped_ = false;
//     have_uncommited_row_ = false;
//   }
//   return ret;
// }

// OB_DEF_SERIALIZE_SIZE(KwDatumRow)
// {
//   int64_t len = 0;
//   LST_DO_CODE(OB_UNIS_ADD_LEN,
//               row_flag_,
//               mvcc_row_flag_,
//               trans_id_,
//               scan_index_,
//               group_idx_,
//               snapshot_version_);
//   OB_UNIS_ADD_LEN_ARRAY(storage_datums_, count_);
//   return len;
// }

// DEF_TO_STRING(KwDatumRow)
// {
//   int64_t pos = 0;
//   J_OBJ_START();
//   J_KV(K_(row_flag), K_(trans_id), K_(scan_index), K_(mvcc_row_flag),
//       K_(snapshot_version), K_(fast_filter_skipped), K_(have_uncommited_row), K_(group_idx), K_(count), K_(datum_buffer));
//   if (NULL != buf && buf_len >= 0) {
//     if (NULL != storage_datums_) {
//       J_ARRAY_START();
//       for (int64_t i = 0; i < count_; ++i) {
//         databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
//         pos += storage_datums_[i].storage_to_string(buf + pos, buf_len - pos);
//         databuff_printf(buf, buf_len, pos, ",");
//       }
//       J_ARRAY_END();
//     }
//   }
//   J_OBJ_END();
//   return pos;
// }

bool KwDatumRow::operator==(const KwDatumRow &other) const
{
  bool is_equal = true;
  if (count_ != other.count_) {
    is_equal = false;
    // STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "datum row count no equal", K(other), K(*this));
  } else {
    for (int64_t i = 0; is_equal && i < count_; i++) {
      is_equal = storage_datums_[i] == other.storage_datums_[i];
      if (!is_equal) {
        // STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "obj and datum no equal", K(i), K(other), K(*this));
      }
    }
  }
  return is_equal;
}

// bool KwDatumRow::operator==(const ObNewRow &other) const
// {
//   bool is_equal = true;
//   if (count_ != other.count_) {
//     is_equal = false;
//     // STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "datum row count no equal", K(other), K(*this));
//   } else {
//     int ret = OB_SUCCESS;
//     for (int64_t i = 0; is_equal && i < count_; i++) {
//       is_equal = storage_datums_[i] == other.cells_[i];
//       if (!is_equal) {
//         // STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "obj and datum no equal", K(i), K(other), K(*this));
//       }
//     }
//   }
//   return is_equal;
// }

/*
 *KwStorageDatumUtils
 */
KwStorageDatumUtils::KwStorageDatumUtils()
  : rowkey_cnt_(0),
    col_cnt_(0),
    cmp_funcs_(),
    // hash_funcs_(),
    // ext_hash_func_(),
    allocator_(nullptr),
    is_oracle_mode_(false),
    is_inited_(false)
{}

KwStorageDatumUtils::KwStorageDatumUtils(int32_t rowkey_cnt, int32_t col_cnt, bool is_oracle_mode, bool is_inited)
  : rowkey_cnt_(rowkey_cnt),
    col_cnt_(col_cnt),
    cmp_funcs_(),
    // hash_funcs_(),
    // ext_hash_func_(),
    allocator_(nullptr),
    is_oracle_mode_(is_oracle_mode),
    is_inited_(is_inited)
{}


KwStorageDatumUtils::~KwStorageDatumUtils()
{}

int KwStorageDatumUtils::transform_multi_version_col_desc(const ObIArray<ObColDesc> &col_descs,
                                                          const int64_t schema_rowkey_cnt,
                                                          ObIArray<ObColDesc> &mv_col_descs)
{
  int ret = 0;//OB_SUCCESS;

  if (/*OB_UNLIKELY*/(schema_rowkey_cnt > col_descs.count())) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to transform mv col descs", K(ret), K(schema_rowkey_cnt), K(col_descs));
  } else {
    mv_col_descs.reuse();
    for (int64_t i = 0; /*OB_SUCC*/ 0 == (ret) && i < schema_rowkey_cnt; i++) {
      if (/*OB_FAIL*/ 0 > (ret = mv_col_descs.push_back(col_descs.at(i)))) {
        // STORAGE_LOG(WARN, "Failed to push back col desc", K(ret), K(i));
      }
    }
    if (/*OB_FAIL*/(0 > ret)) {
    // } else if (/*OB_FAIL*/ 0 > (ret = storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(mv_col_descs))) { 
      // STORAGE_LOG(WARN, "Fail to add extra_rowkey_cols", K(ret), K(schema_rowkey_cnt));
    } else {// 上述函数就是加上2列多版本需要的列
      ObColDesc desc;
      for (int64_t i = 0; 0 == ret && i < 2/*get_extra_rowkey_col_cnt()*/; ++i) {
        desc.col_id_ = i + 7;//common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID = 7, common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID = 8
        desc.col_type_.set_int();
        // by default the trans_version column value would be multiplied by -1
        // so in effect we store the latest version first
        desc.col_order_ = ObOrderType::ASC;
        if (0 > (ret = mv_col_descs.push_back(desc))) {
          STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
        }
      }
      // end of storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(mv_col_descs)
      // need #include "storage/ob_i_store.h"

      for (int64_t i = schema_rowkey_cnt; /*OB_SUCC*/0 == (ret) && i < col_descs.count(); i++) {
        const ObColDesc &col_desc = col_descs.at(i);
        if (col_desc.col_id_ == common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID
            || col_desc.col_id_ == common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
          continue;
        } else if (/*OB_FAIL*/ 0 > (ret = mv_col_descs.push_back(col_desc))) {
          STORAGE_LOG(WARN, "Failed to push back col desc", K(ret), K(col_desc));
        }
      }
    }
  }

  return ret;
}

// int KwStorageDatumUtils::init(const ObIArray<ObColDesc> &col_descs,
//                               const int64_t schema_rowkey_cnt,
//                               const bool is_oracle_mode,
//                               ObIAllocator &allocator)
// {
//   int ret = 0;//OB_SUCCESS;
//   ObSEArray<ObColDesc, 32> mv_col_descs;

//   if (IS_INIT) {
//     ret = -1;//OB_INIT_TWICE;
//     // STORAGE_LOG(WARN, "KwStorageDatumUtils init twice", K(ret), K(*this));
//   } else if (/*OB_UNLIKELY*/(schema_rowkey_cnt < 0 || schema_rowkey_cnt > OB_MAX_ROWKEY_COLUMN_NUMBER
//                   || schema_rowkey_cnt > col_descs.count())) {
//     ret = -2;//OB_INVALID_ARGUMENT;
//     // STORAGE_LOG(WARN, "Invalid argument to init storage datum utils", K(ret), K(col_descs), K(schema_rowkey_cnt));
//   } else if (/*OB_FAIL*/ 0 > (ret = transform_multi_version_col_desc(col_descs, schema_rowkey_cnt, mv_col_descs))) {
//     // STORAGE_LOG(WARN, "Failed to transform multi version col descs", K(ret));
//   } else {
//     is_oracle_mode_ = is_oracle_mode;
//     cmp_funcs_.set_allocator(&allocator);
//     // hash_funcs_.set_allocator(&allocator);
//     if (/*OB_FAIL*/ 0 > (ret = cmp_funcs_.reserve(mv_col_descs.count()))) {
//       // STORAGE_LOG(WARN, "Failed to reserve cmp func array", K(ret));
//     // } else if (/*OB_FAIL*/ 0 > (ret = hash_funcs_.reserve(mv_col_descs.count()))) {
//       // STORAGE_LOG(WARN, "Failed to reserve hash func array", K(ret));
//     } else {
//       // support column order index until next task done
//       //
//       // we could use the cmp funcs in the basic funcs directlly
//       bool is_null_last = is_oracle_mode_;
//       ObCmpFunc cmp_func;
//       // ObHashFunc hash_func;
//       for (int64_t i = 0; /*OB_SUCC*/ 0 == ret && i < mv_col_descs.count(); i++) {
//         const ObColDesc &col_desc = mv_col_descs.at(i);
//         //TODO @hanhui support desc rowkey
//         bool is_ascending = true || col_desc.col_order_ == ObOrderType::ASC;
//         bool has_lob_header = is_lob_storage(col_desc.col_type_.get_type());
//         sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(col_desc.col_type_.get_type(),
//                                                                           col_desc.col_type_.get_collation_type(),
//                                                                           col_desc.col_type_.get_scale(),
//                                                                           is_oracle_mode,
//                                                                           has_lob_header);
//         if (/*OB_UNLIKELY*/(nullptr == basic_funcs
//                        || nullptr == basic_funcs->null_last_cmp_
//                        || nullptr == basic_funcs->murmur_hash_)) {
//           ret = -3;//OB_ERR_SYS;
//           // STORAGE_LOG(ERROR, "Unexpected null basic funcs", K(ret), K(col_desc));
//         } else {
//           cmp_func.cmp_func_ = is_null_last ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
//           // hash_func.hash_func_ = basic_funcs->murmur_hash_;
//           // if (/*OB_FAIL*/ 0 > (ret = hash_funcs_.push_back(hash_func))) {
//             // STORAGE_LOG(WARN, "Failed to push back hash func", K(ret), K(i), K(col_desc));
//           // } else if (is_ascending) {
//             if (is_ascending && /*OB_FAIL*/ 0 > (ret = cmp_funcs_.push_back(KwStorageDatumCmpFunc(cmp_func)))) {
//               // STORAGE_LOG(WARN, "Failed to push back cmp func", K(ret), K(i), K(col_desc));
//             // }
//           } else {
//             ret = -4; //OB_ERR_SYS;
//             // STORAGE_LOG(WARN, "Unsupported desc column order", K(ret), K(col_desc), K(i));
//           }
//         }
//       }
//     }
//     if (/*OB_SUCC*/ 0 == ret) {
//       // sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObExtendType, CS_TYPE_BINARY);
//       // if (/*OB_UNLIKELY*/(nullptr == basic_funcs || nullptr == basic_funcs->murmur_hash_)) {
//       //   ret = -5;//OB_ERR_SYS;
//       //   // STORAGE_LOG(ERROR, "Unexpected null basic funcs for extend type", K(ret));
//       // } else {
//         // ext_hash_func_.hash_func_ = basic_funcs->murmur_hash_;
//         rowkey_cnt_ = schema_rowkey_cnt + 2;//storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
//         col_cnt_ = mv_col_descs.count();
//         allocator_ = &allocator;
//         is_inited_ = true;
//       // }
//     }

//   }

//   return ret;
// }

int KwStorageDatumUtils::init(const ObIArray<ObColDesc> &col_descs,
                              const int64_t schema_rowkey_cnt,
                              const bool is_oracle_mode,
                              ObIAllocator &allocator)
{
  int ret = 0;//OB_SUCCESS;
  ObSEArray<ObColDesc, 32> mv_col_descs;

  if (IS_INIT) {
    ret = -1;//OB_INIT_TWICE;
    // STORAGE_LOG(WARN, "KwStorageDatumUtils init twice", K(ret), K(*this));
  } else if (/*OB_UNLIKELY*/(schema_rowkey_cnt < 0 || schema_rowkey_cnt > OB_MAX_ROWKEY_COLUMN_NUMBER
                  || schema_rowkey_cnt > col_descs.count())) {
    ret = -2;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to init storage datum utils", K(ret), K(col_descs), K(schema_rowkey_cnt));
  } else if (/*OB_FAIL*/ 0 > (ret = transform_multi_version_col_desc(col_descs, schema_rowkey_cnt, mv_col_descs))) {
    // STORAGE_LOG(WARN, "Failed to transform multi version col descs", K(ret));
  } else {
    is_oracle_mode_ = is_oracle_mode;
    cmp_funcs_.set_allocator(&allocator);
    // hash_funcs_.set_allocator(&allocator);
    if (/*OB_FAIL*/ 0 > (ret = cmp_funcs_.reserve(mv_col_descs.count()))) {
      // STORAGE_LOG(WARN, "Failed to reserve cmp func array", K(ret));
    // } else if (/*OB_FAIL*/ 0 > (ret = hash_funcs_.reserve(mv_col_descs.count()))) {
      // STORAGE_LOG(WARN, "Failed to reserve hash func array", K(ret));
    } else {
      // support column order index until next task done
      //
      // we could use the cmp funcs in the basic funcs directlly
      bool is_null_last = is_oracle_mode_;
      ObCmpFunc cmp_func;
      // ObHashFunc hash_func;
      for (int64_t i = 0; /*OB_SUCC*/ 0 == ret && i < mv_col_descs.count(); i++) {
        const ObColDesc &col_desc = mv_col_descs.at(i);
        //TODO @hanhui support desc rowkey
        bool is_ascending = true || col_desc.col_order_ == ObOrderType::ASC;
        bool has_lob_header = is_lob_storage(col_desc.col_type_.get_type());
        ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(col_desc.col_type_.get_type(),
                                                                          col_desc.col_type_.get_collation_type(),
                                                                          col_desc.col_type_.get_scale(),
                                                                          is_oracle_mode,
                                                                          has_lob_header);
        if (/*OB_UNLIKELY*/(nullptr == basic_funcs
                       || nullptr == basic_funcs->null_last_cmp_)) {
                      //  || nullptr == basic_funcs->murmur_hash_)) {
          ret = -3;//OB_ERR_SYS;
          // STORAGE_LOG(ERROR, "Unexpected null basic funcs", K(ret), K(col_desc));
        } else {
          cmp_func.cmp_func_ = is_null_last ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
          // hash_func.hash_func_ = basic_funcs->murmur_hash_;
          // if (/*OB_FAIL*/ 0 > (ret = hash_funcs_.push_back(hash_func))) {
            // STORAGE_LOG(WARN, "Failed to push back hash func", K(ret), K(i), K(col_desc));
          // } else if (is_ascending) {
            if (is_ascending && /*OB_FAIL*/ 0 > (ret = cmp_funcs_.push_back(KwStorageDatumCmpFunc(cmp_func)))) {
              // STORAGE_LOG(WARN, "Failed to push back cmp func", K(ret), K(i), K(col_desc));
            // }
          } else {
            ret = -4; //OB_ERR_SYS;
            // STORAGE_LOG(WARN, "Unsupported desc column order", K(ret), K(col_desc), K(i));
          }
        }
      }
    }
    if (/*OB_SUCC*/ 0 == ret) {
      // sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObExtendType, CS_TYPE_BINARY);
      // if (/*OB_UNLIKELY*/(nullptr == basic_funcs || nullptr == basic_funcs->murmur_hash_)) {
      //   ret = -5;//OB_ERR_SYS;
      //   // STORAGE_LOG(ERROR, "Unexpected null basic funcs for extend type", K(ret));
      // } else {
        // ext_hash_func_.hash_func_ = basic_funcs->murmur_hash_;
        rowkey_cnt_ = schema_rowkey_cnt + 2;//storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        col_cnt_ = mv_col_descs.count();
        allocator_ = &allocator;
        is_inited_ = true;
      // }
    }

  }

  return ret;
}

void KwStorageDatumUtils::reset()
{
  rowkey_cnt_ = 0;
  col_cnt_ = 0;
  cmp_funcs_.reset();
  // hash_funcs_.reset();
  allocator_ = nullptr;
  // ext_hash_func_.hash_func_ = nullptr;
  is_inited_ = false;
}

int KwGhostRowUtil::is_ghost_row(
    const KwMultiVersionRowFlag &flag,
    bool &is_ghost_row)
{
  int ret = 0;//OB_SUCCESS;
  is_ghost_row = false;
  if (flag.is_ghost_row()) {
    is_ghost_row = true;
    if (OB_UNLIKELY(!flag.is_last_multi_version_row())) {
      ret = OB_ERR_UNEXPECTED;
      // FLOG_ERROR("ghost row should only be last row", K(ret), K(flag));
      // 需要#include "share/ob_force_print_log.h"
    }
  }
  return ret;
}

// int KwGhostRowUtil::make_ghost_row(
//     const int64_t sql_sequence_col_idx,
//     const ObQueryFlag &query_flag,
//     KwDatumRow &row)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY((!query_flag.is_sstable_cut()
//                    && (!row.mvcc_row_flag_.is_uncommitted_row() || !row.trans_id_.is_valid()))
//                   || !row.mvcc_row_flag_.is_last_multi_version_row()
//                   || row.get_column_count() < sql_sequence_col_idx)) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(WARN, "invalid argument", K(ret), K(row), K(sql_sequence_col_idx));
//   } else {
//     row.row_flag_.set_flag(KwDmlFlag::KW_DF_UPDATE);
//     row.mvcc_row_flag_.reset();
//     row.mvcc_row_flag_.set_ghost_row(true);
//     row.mvcc_row_flag_.set_last_multi_version_row(true);
//     row.trans_id_.reset();
//     row.storage_datums_[sql_sequence_col_idx - 1].set_int(GHOST_NUM);
//     row.storage_datums_[sql_sequence_col_idx].set_int(GHOST_NUM);
//     for (int i = sql_sequence_col_idx + 1; i < row.get_column_count(); ++i) {
//       row.storage_datums_[i].set_nop();
//     }
//   }
//   return ret;
// }

void kw_format_mvcc_str(const int32_t flag, char *str, int len)
{
  assert(len >= 16);
  int32_t bit;
  int32_t count = 0;
  uint32_t f = flag;
  memset(str, 0, len);
  while (0 != f && count < KwMvccFlagCount) {
    bit = __builtin_ffs(f);
    if (0 < count) {
      strncat(str, "|", 16 - strlen(str));
    }
    strncat(str, KwMvccFlagStr[bit], 16 - strlen(str));
    f = f & (0xFFFFFFFF << bit);
    count++;
  }
}

} // namespace blocksstable
} // namespace oceanbase