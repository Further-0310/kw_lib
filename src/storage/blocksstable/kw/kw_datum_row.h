#ifndef KW_STORAGE_BLOCKSSTABLE_DATUM_ROW_H
#define KW_STORAGE_BLOCKSSTABLE_DATUM_ROW_H
// #include <stdint.h>
// #include "kw_datum.h"

// #include "common/ob_tablet_id.h"
// // #include "common/row/ob_row.h"
// #include "storage/ob_storage_util.h"

// #include "share/schema/ob_table_param.h"
// #include "kw_block_sstable_struct.h"
#include "kw_table_param.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
// #include "storage/tx/ob_trans_define.h"


namespace oceanbase
{
// namespace share{
// namespace schema
// {
// struct ObColDesc;
// }
// }
// namespace storage
// {
// struct ObStoreRow;
// }

namespace blocksstable
{

enum KwDmlFlag
{
  KW_DF_NOT_EXIST = 0,
  KW_DF_LOCK = 1,
  KW_DF_UPDATE = 2,
  KW_DF_INSERT = 3,
  KW_DF_DELETE = 4,
  KW_DF_MAX = 5,
};

static const char *KwDmlFlagStr[KW_DF_MAX] = {
    "NOT_EXIST",
    "LOCK",
    "UPDATE",
    "INSERT",
    "DELETE"
};

enum KwDmlRowFlagType
{
  KW_DF_TYPE_NORMAL = 0,
  KW_DF_TYPE_INSERT_DELETE = 1,
  KW_DF_TYPE_MAX,
};

static const char *KwDmlTypeStr[KW_DF_TYPE_MAX] = {
    "N",
    "I_D"
};

const char *kw_get_dml_str(KwDmlFlag dml_flag);
void kw_format_dml_str(const int32_t flag, char *str, int len);

struct KwDmlRowFlag
{
  // OB_UNIS_VERSION(1);
public:
  KwDmlRowFlag() : whole_flag_(0) {}
  KwDmlRowFlag(const uint8_t flag) : whole_flag_(flag) {}
  KwDmlRowFlag(KwDmlFlag flag) : whole_flag_(0) {}
  ~KwDmlRowFlag() {}
  inline void reset() { whole_flag_ = 0; }
  inline void set_flag(KwDmlFlag flag){ if(flag >= KW_DF_NOT_EXIST && flag < KW_DF_MAX){ flag_ = flag; } }
  inline bool is_delete() const { return KW_DF_DELETE == flag_; }
  inline bool is_lock() const { return KW_DF_LOCK == flag_; }
  inline bool is_not_exist() const { return KW_DF_NOT_EXIST == flag_; }
  inline bool is_insert() const { return KW_DF_INSERT == flag_; }
  inline bool is_update() const { return KW_DF_UPDATE == flag_; }
  inline bool is_valid() const { return (KW_DF_TYPE_NORMAL == flag_type_ && KW_DF_DELETE >= flag_) || (KW_DF_TYPE_INSERT_DELETE == flag_type_ && KW_DF_DELETE == flag_); }
  inline bool is_exist() const { return is_valid() && !is_not_exist(); }
  inline bool is_exist_without_delete() const { return is_exist() && !is_delete(); }
  inline bool is_extra_delete() const { return KW_DF_TYPE_INSERT_DELETE != flag_type_ && KW_DF_DELETE == flag_; }
  inline bool is_insert_delete() const { return KW_DF_TYPE_INSERT_DELETE == flag_type_ && KW_DF_DELETE == flag_; }
  inline void fuse_flag(const KwDmlRowFlag input_flag)
  {
    if(input_flag.is_valid()){
      if(KW_DF_INSERT == input_flag.flag_){
        if(KW_DF_DELETE == flag_){ flag_type_ = KW_DF_TYPE_INSERT_DELETE; }
        else { flag_ = KW_DF_INSERT; }
      }
      else if( KW_DF_DELETE == input_flag.flag_ && KW_DF_DELETE == flag_){
        if(flag_type_ == KW_DF_TYPE_INSERT_DELETE){ flag_type_ = input_flag.flag_type_; }
        else{}//debug "unexpected pure delete row"
      }
    }
  }
  inline uint8_t get_serialize_flag() const { return whole_flag_; }// use when Serialize or print
  inline KwDmlFlag get_dml_flag() const { return (KwDmlFlag)flag_; }
  KwDmlRowFlag & operator = (const KwDmlRowFlag &other)
  {
    if(other.is_valid()){ whole_flag_ = other.whole_flag_; }
    return *this;
  }
/* csy: transfer from & to ObDmlRowFlag
  KwDmlRowFlag & operator = (const ObDmlRowFlag &other)
  {
    if(other.is_valid()){ whole_flag_ = other.get_serialize_flag(); }
    return *this;
  }
  int to_ob_dml_row_flag(ObDmlRowFlag &ob_row_flag) 
  {
    int ret = 0;
    if(ob_row_flag.is_valid()) {
      ob_row_flag = ObDmlRowFlag(whole_flag_);
    }
    return ret;
  }
*/ 

  const char *getFlagStr() const
  {
    const char *str = nullptr;
    if(is_valid()){ str = KwDmlFlagStr[flag_]; }
    else { str = "invalid flag"; }
    return str;
  }
  inline void format_str(char *str, int8_t len) const { return kw_format_dml_str(whole_flag_, str, len); }

private:
  bool operator != (const KwDmlRowFlag &other) const { return flag_ != other.flag_; } // for unittest

  const static uint8_t KW_FLAG_TYPE_MASK = 0x80;
  const static uint8_t KW_FLAG_MASK = 0x7F;
  union
  {
    uint8_t whole_flag_;
    struct {
      uint8_t flag_      : 7;  // store KwDmlFlag
      uint8_t flag_type_ : 1;  // mark is pure_delete or insert_delete
    };
  };
};

static const int8_t KwMvccFlagCount = 8;
static const char *KwMvccFlagStr[KwMvccFlagCount] = {
  "",
  "F",
  "U",
  "S",
  "C",
  "G",
  "L",
  "UNKNOWN"
};
void kw_format_mvcc_str(const int32_t flag, char *str, int len);

struct KwMultiVersionRowFlag
{
  // OB_UNIS_VERSION(1);
public:
  union
  {
    uint8_t flag_;
    struct
    {
      uint8_t is_first_        : 1;    // 0: not first row(default), 1: first row
      uint8_t is_uncommitted_  : 1;    // 0: committed(default), 1: uncommitted row
      uint8_t is_shadow_       : 1;    // 0: not new compacted shadow row(default), 1: shadow row
      uint8_t is_compacted_    : 1;    // 0: multi_version_row(default), 1: compacted_multi_version_row
      uint8_t is_ghost_        : 1;    // 0: not ghost row(default), 1: ghost row
      uint8_t is_last_         : 1;    // 0: not last row(default), 1: last row
      uint8_t reserved_        : 2;
    };
  };

  KwMultiVersionRowFlag() : flag_(0) {}
  KwMultiVersionRowFlag(uint8_t flag) : flag_(flag) {}
  void reset() { flag_ = 0; }
  inline void set_compacted_multi_version_row(const bool is_compacted_multi_version_row){ is_compacted_ = is_compacted_multi_version_row; }
  inline void set_last_multi_version_row(const bool is_last_multi_version_row){ is_last_ = is_last_multi_version_row; }
  inline void set_first_multi_version_row(const bool is_first_multi_version_row){ is_first_ = is_first_multi_version_row; }
  inline void set_uncommitted_row(const bool is_uncommitted_row){ is_uncommitted_ = is_uncommitted_row; }
  inline void set_ghost_row(const bool is_ghost_row){ is_ghost_ = is_ghost_row; }
  inline void set_shadow_row(const bool is_shadow_row){ is_shadow_ = is_shadow_row; }
  inline bool is_compacted_multi_version_row() const { return is_compacted_; }
  inline bool is_last_multi_version_row() const { return is_last_; }
  inline bool is_first_multi_version_row() const { return is_first_; }
  inline bool is_uncommitted_row() const { return is_uncommitted_; }
  inline bool is_ghost_row() const { return is_ghost_; }
  inline bool is_shadow_row() const { return is_shadow_; }
  inline void format_str(char *str, int8_t len) const { return kw_format_mvcc_str(flag_, str, len); }
/* csy : transfer from & to ObMultiVersionRowFlag
  KwMultiVersionRowFlag & operator = (const ObMultiVersionRowFlag &other)
  {
    flag_ = other.flag_;
    return *this;
  }
  int to_ob_multi_version_row_flag(ObMultiVersionRowFlag &ob_multi_version_row_flag) 
  {
    int ret = 0;
    ob_multi_version_row_flag.flag_ = flag_;
    return ret;
  }
*/

  TO_STRING_KV("first", is_first_,
               "uncommitted", is_uncommitted_,
               "shadow", is_shadow_,
               "compact", is_compacted_,
               "ghost", is_ghost_,
               "last", is_last_,
               "reserved", reserved_,
               K_(flag));

};


//TODO optimize number buffer
struct KwStorageDatum : public common::ObDatum
{
  KwStorageDatum() { set_nop(); }
  KwStorageDatum(const KwStorageDatum &datum) { reuse(); *this = datum; }

  ~KwStorageDatum() = default;
  // ext value section
  inline void reuse() { ptr_ = buf_; reserved_ = 0; pack_ = 0; }
  inline void set_ext_value(const int64_t ext_value) { reuse(); set_ext(); no_cv(extend_obj_)->set_ext(ext_value); }
  inline void set_nop() { set_ext_value(ObActionFlag::OP_NOP); }
  inline void set_min() { set_ext_value(common::ObObj::MIN_OBJECT_VALUE); }
  inline void set_max() { set_ext_value(common::ObObj::MAX_OBJECT_VALUE); }
  inline bool is_nop_value() const { return is_nop(); } // temp solution
  // transfer section
  inline bool is_local_buf() const { return ptr_ == buf_; }
  inline int from_buf_enhance(const char *buf, const int64_t buf_len);
  inline int from_obj_enhance(const common::ObObj &obj);
  inline int to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const;
  inline int deep_copy(const KwStorageDatum &src, common::ObIAllocator &allocator);
  inline int deep_copy(const KwStorageDatum &src, char * buf, const int64_t buf_len, int64_t &pos);
  inline int64_t get_deep_copy_size() const;
  inline KwStorageDatum& operator=(const KwStorageDatum &other);
  inline int64_t storage_to_string(char *buf, int64_t buf_len) const;
  inline bool need_copy_for_encoding_column_with_flat_format(const ObObjDatumMapType map_type) const;
  //only for unittest
  inline bool operator==(const KwStorageDatum &other) const;
  inline bool operator==(const ObObj &other) const;

  //datum 12 byte
  int32_t reserved_;
  //buf 16 byte
  char buf_[common::OBJ_DATUM_NUMBER_RES_SIZE];
};

struct KwStorageDatumBuffer
{
public:
  KwStorageDatumBuffer(common::ObIAllocator *allocator = nullptr);
  ~KwStorageDatumBuffer() { reset(); }
  void reset();
  int init(common::ObIAllocator &allocator);
  int reserve(const int64_t count, const bool keep_data = false);
  inline bool is_valid() const { return is_inited_; }
  inline KwStorageDatum *get_datums() { return datums_; }
  inline int64_t get_capacity() const { return capacity_; }
  // TO_STRING_KV(K_(capacity), KP_(datums), KP_(local_datums));
private:
  static const int64_t LOCAL_BUFFER_ARRAY = common::OB_ROW_DEFAULT_COLUMNS_COUNT;
  int64_t capacity_;
  KwStorageDatum local_datums_[LOCAL_BUFFER_ARRAY];
  KwStorageDatum *datums_;
  common::ObIAllocator *allocator_;
  bool is_inited_;
};


struct KwDatumRow
{
public:
    KwDatumRow();
    ~KwDatumRow();
    int init(oceanbase::common::ObIAllocator &allocator, const int64_t capacity);
    int init(const int64_t capacity);
    void reset();
    void reuse();
    int reserve(const int64_t capacity, const bool keep_data = false);
    int deep_copy(const KwDatumRow &src, common::ObIAllocator &allocator);
    //TODO need remove by @hanhui
    // int prepare_new_row(const common::ObIArray<share::schema::ObColDesc> &out_cols);
    // int to_store_row(const common::ObIArray<share::schema::ObColDesc> &out_cols, storage::ObStoreRow &store_row);
    // int from_store_row(const storage::ObStoreRow &store_row);
    //only for unittest
    bool operator==(const KwDatumRow &other) const;
    // bool operator==(const common::ObNewRow &other) const;

    // inline ObNewRow &get_new_row() { return old_row_; }
    // inline const ObNewRow &get_new_row() const { return old_row_; }
    inline int64_t get_capacity() const { return datum_buffer_.get_capacity(); }
    inline int64_t get_column_count() const { return count_; }
    inline bool is_valid() const { return nullptr != storage_datums_ && get_capacity() > 0; }
    // multi version row section
    // inline transaction::ObTransID get_trans_id() const { return trans_id_; }
    inline int64_t get_trans_id() const { return trans_id_; }
    // inline void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
    inline void set_trans_id(const int64_t &trans_id) { trans_id_ = trans_id; }
    inline bool is_have_uncommited_row() const { return have_uncommited_row_; }
    inline void set_have_uncommited_row(const bool have_uncommited_row = true) { have_uncommited_row_ = have_uncommited_row; }
    inline bool is_ghost_row() const { return mvcc_row_flag_.is_ghost_row(); }
    inline bool is_uncommitted_row() const { return mvcc_row_flag_.is_uncommitted_row(); }
    inline bool is_compacted_multi_version_row() const { return mvcc_row_flag_.is_compacted_multi_version_row(); }
    inline bool is_first_multi_version_row() const { return mvcc_row_flag_.is_first_multi_version_row(); }
    inline bool is_last_multi_version_row() const { return mvcc_row_flag_.is_last_multi_version_row(); }
    inline bool is_shadow_row() const { return mvcc_row_flag_.is_shadow_row(); }
    inline void set_compacted_multi_version_row() { mvcc_row_flag_.set_compacted_multi_version_row(true); }
    inline void set_first_multi_version_row() { mvcc_row_flag_.set_first_multi_version_row(true); }
    inline void set_last_multi_version_row() { mvcc_row_flag_.set_last_multi_version_row(true); }
    inline void set_shadow_row() { mvcc_row_flag_.set_shadow_row(true); }
    inline void set_multi_version_flag(const KwMultiVersionRowFlag &multi_version_flag) { mvcc_row_flag_ = multi_version_flag; }
    // row estimate section
    inline int32_t get_delta() const
    {
      int32_t delta = 0;
      if(row_flag_.is_extra_delete()){ delta = -1; }
      else if(row_flag_.is_insert()){ delta = 1; }
      return delta;
    }

    // DECLARE_TO_STRING;
    TO_STRING_KV(K_(count), "datums_:", ObArrayWrap<ObDatum>(storage_datums_, count_));
public:
    common::ObArenaAllocator local_allocator_;
    uint16_t count_;
    bool fast_filter_skipped_;
    bool have_uncommited_row_;
    KwDmlRowFlag row_flag_;
    KwMultiVersionRowFlag mvcc_row_flag_;
    // transaction::ObTransID trans_id_; //先用int代替
    int64_t trans_id_;
    int64_t scan_index_;
    int64_t group_idx_;
    int64_t snapshot_version_;
    KwStorageDatum *storage_datums_;
    // do not need serialize
    KwStorageDatumBuffer datum_buffer_;
    //TODO @hanhui only for compile
    // common::ObNewRow old_row_;
    // storage::ObObjBufArray obj_buf_;
};

struct KwConstDatumRow
{
  OB_UNIS_VERSION(1);
public:
  KwConstDatumRow() { MEMSET(this, 0, sizeof(KwConstDatumRow)); }
  KwConstDatumRow(const ObDatum *datums, uint64_t count)
    : datums_(datums), count_(count) {}
  ~KwConstDatumRow() {}
  inline int64_t get_column_count() const { return count_; }
  inline bool is_valid() const { return nullptr != datums_ && count_ > 0; }
  inline const ObDatum &get_datum(const int64_t col_idx) const
  {
    /*OB_ASSERT*/assert(col_idx < count_ && col_idx >= 0);
    return datums_[col_idx];
  }
  TO_STRING_KV(K_(count), "datums_:", ObArrayWrap<ObDatum>(datums_, count_));
  const ObDatum *datums_;
  uint64_t count_; 
};

struct KwStorageDatumCmpFunc
{
public:
  KwStorageDatumCmpFunc(common::ObCmpFunc &cmp_func) : cmp_func_(cmp_func) {}
  KwStorageDatumCmpFunc() = default;
  ~KwStorageDatumCmpFunc() = default;
  int compare(const KwStorageDatum &left, const KwStorageDatum &right, int &cmp_ret) const;
  TO_STRING_KV(K_(cmp_func));
private:
  common::ObCmpFunc cmp_func_;
};

typedef common::ObFixedArray<KwStorageDatumCmpFunc, common::ObIAllocator> KwStoreCmpFuncs;
struct KwStorageDatumUtils
{
  KwStorageDatumUtils();
  KwStorageDatumUtils(int32_t rowkey_cnt, int32_t col_cnt, bool is_oracle_mode, bool is_inited);
  ~KwStorageDatumUtils();
  int init(const common::ObIArray<ObColDesc> &col_descs,
           const int64_t schema_rowkey_cnt,
           const bool is_oracle_mode,
           common::ObIAllocator &allocator);
  void reset();
  inline bool is_valid() const { return is_inited_; }
  inline bool is_oracle_mode() const { return is_oracle_mode_; }
  inline int64_t get_rowkey_count() const { return rowkey_cnt_; }
  inline int64_t get_column_count() const { return col_cnt_; }
  inline const KwStoreCmpFuncs &get_cmp_funcs() const { return cmp_funcs_; }
  // inline const common::ObHashFuncs &get_hash_funcs() const { return hash_funcs_; }
  // inline const common::ObHashFunc &get_ext_hash_funcs() const { return ext_hash_func_; }
  TO_STRING_KV(K_(is_oracle_mode), K_(rowkey_cnt), K_(col_cnt), KP_(allocator), K_(is_inited));
  // inline static const KwStorageDatumUtils from_ob(int32_t rowkey_cnt, int32_t col_cnt, bool is_oracle_mode, bool is_inited)
  // { return KwStorageDatumUtils(rowkey_cnt, col_cnt, is_oracle_mode, is_inited); }
private:
  //TODO to be removed by @hanhui
  int transform_multi_version_col_desc(const common::ObIArray<ObColDesc> &col_descs,
                                       const int64_t schema_rowkey_cnt,
                                       common::ObIArray<ObColDesc> &mv_col_descs);

private:
  int32_t rowkey_cnt_;
  int32_t col_cnt_;
  KwStoreCmpFuncs cmp_funcs_;
  // common::ObHashFuncs hash_funcs_;
  // common::ObHashFunc ext_hash_func_;
  ObIAllocator *allocator_;
  bool is_oracle_mode_;
  bool is_inited_;
  // DISALLOW_COPY_AND_ASSIGN(KwStorageDatumUtils);
};

inline int KwStorageDatum::deep_copy(const KwStorageDatum &src, common::ObIAllocator &allocator)
{
  int ret = 0; //OB_SUCCESS
  reuse();
  pack_ = src.pack_;
  if(is_null()) {}
  else if(src.len_ == 0) {}
  else if(src.is_local_buf()) {
    assert(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    memcpy(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  }
  else {
    char *buf = static_cast<char *>(allocator.alloc(src.len_));
    if(NULL == buf) { ret = -1; } // OB_ALLOCATE_MEMORY_FAILED
    else{
      memcpy(buf, src.ptr_, src.len_);
      // need set ptr_ after memory copy, if this == &src
      ptr_ = buf;
    }
  }
  return ret;
}

inline int KwStorageDatum::deep_copy(const KwStorageDatum &src, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = 0;
  reuse();
  pack_ = src.pack_;
  if(is_null()) {}
  else if(src.len_ == 0){}
  else if(src.is_local_buf()){
    assert(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    memcpy(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  }
  else if(nullptr == buf || buf_len < pos + src.len_) { ret = -1; } // OB_INVALID_ARGUMENT
  else {
    memcpy(buf + pos, src.ptr_, src.len_);
    // need set ptr_ after memory copy, if this == &src
    ptr_ = buf + pos;
    pos += src.len_;
  }
  return ret;
}

inline int64_t KwStorageDatum::get_deep_copy_size() const
{
  int64_t deep_copy_len = 0;
  if(is_null()) {}
  else if(is_local_buf()) { assert(len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE); }
  else { deep_copy_len = len_; }
  return deep_copy_len;
}

inline int KwStorageDatum::from_buf_enhance(const char *buf, const int64_t buf_len)
{
  int ret = 0;
  if(nullptr == buf || buf_len < 0 || buf_len > UINT32_MAX) { ret = -1; } // OB_INVALID_ARGUMENT
  else {
    reuse();
    len_ = static_cast<uint32_t>(buf_len);
    if(buf_len > 0) { ptr_ = buf; }
  }
  return ret;
}

inline int KwStorageDatum::from_obj_enhance(const common::ObObj &obj)
{
  int ret = 0;
  if(obj.is_ext()) { set_ext_value(obj.get_ext()); }
  else if( 0 > from_obj(obj)) { ret = -1; } // warn "Failed to transfer obj to datum"
  // STORAGE_LOG(DEBUG, "chaser debug from obj", K(obj), K(*this));
  return ret;
}

inline int KwStorageDatum::to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const
{
  int ret = 0;
  if(is_outrow()) { ret = -1; } // OB_ERR_UNEXPECTED
  else if(is_ext()) { obj.set_ext(get_ext()); }
  else if(0 > to_obj(obj, meta)) { ret = -2; } // warn "Failed to transfer datum to obj"
  return ret;
}

inline KwStorageDatum& KwStorageDatum::operator=(const KwStorageDatum &other)
{
  if(&other != this){
    reuse();
    pack_ = other.pack_;
    if(is_null()){}
    else if(len_ == 0){}
    else if(other.is_local_buf()){
      assert(other.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
      memcpy(buf_, other.ptr_, other.len_);
      ptr_ = buf_;
    }
    else { ptr_ = other.ptr_; }
  }
  return *this;
}

inline bool KwStorageDatum::operator==(const KwStorageDatum &other) const 
{
  bool bret = true;
  if(is_null()) { bret = other.is_null(); }
  else if(is_ext()) { bret = other.is_ext() && extend_obj_->get_ext() == other.extend_obj_->get_ext(); }
  else { bret = ObDatum::binary_equal(*this, other); }

  if(!bret) {} //STORAGE_LOG(DEBUG, "obj and datum no equal", K(other), K(*this));

  return bret; 
}

inline bool KwStorageDatum::operator==(const common::ObObj &other) const
{
  int ret = 0;
  bool bret = true;
  KwStorageDatum datum;
  if(0 > datum.from_obj_enhance(other)) { ret = -1; } // warn "Failed to transfer obj to datum"
  else { bret = *this == datum; }

  if(!bret) {} //STORAGE_LOG(DEBUG, "obj and datum no equal", K(other), K(datum), KPC(this));

  return bret;
}

inline int64_t KwStorageDatum::storage_to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  if(is_ext()) {
    if(is_nop()) { J_NOP(); }
    else if(is_max()) { BUF_PRINTF("MAX_OBJ"); }
    else if(is_min()) { BUF_PRINTF("MIN_OBJ"); }
  }
  else { pos = to_string(buf, buf_len); }
  return pos;
}

inline bool KwStorageDatum::need_copy_for_encoding_column_with_flat_format(const ObObjDatumMapType map_type) const
{
  return OBJ_DATUM_STRING == map_type && sizeof(uint64_t) == len_ && is_local_buf();
}

struct KwGhostRowUtil
{
  public:
  KwGhostRowUtil() = delete;
  ~KwGhostRowUtil() = delete;
  // static int make_ghost_row(
  //     const int64_t sql_sequence_col_idx,
  //     const common::ObQueryFlag &query_flag,
  //     KwDatumRow &row);
  // need #include "common/ob_common_types.h"
  static int is_ghost_row(const KwMultiVersionRowFlag &flag, bool &is_ghost_row);
  static const int64_t GHOST_NUM = INT64_MAX;
};


} // namespace blocksstable
} // namespace kingwow
#endif