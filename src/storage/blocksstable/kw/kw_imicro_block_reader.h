#ifndef KW_STORAGE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_
#define KW_STORAGE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_

#include "kw_table_read_info.h"
#include "kw_block_sstable_struct.h"
#include "kw_datum_range.h"
#include "kw_micro_block_header.h"
// #include "common/ob_store_format.h"
// #include "storage/blocksstable/ob_index_block_row_struct.h" 需要ObMicroIndexInfo

namespace oceanbase
{
using namespace common;

namespace blocksstable
{

// template<typename T>
// class KwMicroBlockAggInfo {
// public:
//   KwMicroBlockAggInfo(bool is_min, const ObDatumCmpFuncType cmp_fun, T &result_datum) :
//       is_min_(is_min), cmp_fun_(cmp_fun), result_datum_(result_datum) {}
//   void update_min_or_max(const T& datum)
//   {
//     if (datum.is_null()) {
//     } else if (result_datum_.is_null()) {
//       result_datum_ = datum;
//     } else {
//       int cmp_ret = cmp_fun_(result_datum_, datum);
//       if ((is_min_ && cmp_ret > 0) || (!is_min_ && cmp_ret < 0)) {
//         result_datum_ = datum;
//       }
//     }
//   }
//   // TO_STRING_KV(K_(is_min), K_(cmp_fun), K_(result_datum));
// private:
//   bool is_min_;
//   const ObDatumCmpFuncType cmp_fun_;
//   T &result_datum_;
// };

struct KwRowIndexIterator
{
public:
  typedef KwRowIndexIterator self_t;
  typedef std::random_access_iterator_tag iterator_category;
  typedef int64_t value_type;
  typedef int64_t difference_type;
  typedef int64_t *pointer;
  typedef int64_t &reference;

  static const self_t &invalid_iterator()
  { static self_t invalid_iter(INT64_MIN); return invalid_iter; }

  KwRowIndexIterator() : row_id_(0) {}
  explicit KwRowIndexIterator(const int64_t id) : row_id_(id) {}

  int64_t operator *() const { return row_id_; }
  bool operator ==(const self_t &r) const { return row_id_ == r.row_id_; }
  bool operator !=(const self_t &r) const { return row_id_ != r.row_id_; }
  bool operator <(const self_t &r) const { return row_id_ < r.row_id_; }
  bool operator >(const self_t &r) const { return row_id_ > r.row_id_; }
  bool operator >=(const self_t &r) const { return row_id_ >= r.row_id_; }
  bool operator <=(const self_t &r) const { return row_id_ <= r.row_id_; }
  difference_type operator -(const self_t &r) const { return row_id_ - r.row_id_; }
  self_t operator -(difference_type step) const { return self_t(row_id_ - step); }
  self_t operator +(difference_type step) const { return self_t(row_id_ + step); }
  self_t &operator -=(difference_type step) { row_id_ -= step; return *this; }
  self_t &operator +=(difference_type step) { row_id_ += step; return *this; }
  self_t &operator ++() { row_id_ ++; return *this; }
  self_t operator ++(int) { return self_t(row_id_++); }
  self_t &operator --() { row_id_ --; return *this; }
  self_t operator --(int) { return self_t(row_id_--); }

  // TO_STRING_KV(K_(row_id));
  int64_t row_id_;
};

struct KwMicroBlockData
{
  enum Type
  {
    DATA_BLOCK,
    INDEX_BLOCK,
    DDL_BLOCK_TREE,
    MAX_TYPE
  };
public:
  KwMicroBlockData(): buf_(NULL), size_(0), extra_buf_(0), extra_size_(0), type_(DATA_BLOCK) {}
  KwMicroBlockData(const char *buf,
                   const int64_t size,
                   const char *extra_buf = nullptr,
                   const int64_t extra_size = 0,
                   const Type block_type = DATA_BLOCK)
      : buf_(buf), size_(size), extra_buf_(extra_buf), extra_size_(extra_size), type_(block_type) {}
  bool is_valid() const { return NULL != buf_ && size_ > 0 && type_ < MAX_TYPE; }
  const char *&get_buf() { return buf_; }
  const char *get_buf() const { return buf_; }
  int64_t &get_buf_size() { return size_; }
  int64_t get_buf_size() const { return size_; }

  const char *&get_extra_buf() { return extra_buf_; }
  const char *get_extra_buf() const { return extra_buf_; }
  int64_t get_extra_size() const { return extra_size_; }
  int64_t &get_extra_size() { return extra_size_; }

  int64_t total_size() const { return size_ + extra_size_; }
  bool is_index_block() const { return INDEX_BLOCK == type_ || DDL_BLOCK_TREE == type_;}

  void reset() { *this = KwMicroBlockData(); }
  inline const KwMicroBlockHeader *get_micro_header() const
  {
    const KwMicroBlockHeader *micro_header = reinterpret_cast<const KwMicroBlockHeader *>(buf_);
    const bool is_valid_micro_header =
        size_ >= KwMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET  && micro_header->is_valid();
    return is_valid_micro_header ? micro_header : nullptr;
  }
  inline ObRowStoreType get_store_type() const
  {
    const KwMicroBlockHeader *micro_header = reinterpret_cast<const KwMicroBlockHeader *>(buf_);
    const bool is_valid_micro_header =
        size_ >= KwMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET && micro_header->is_valid();
    return is_valid_micro_header
      ? static_cast<ObRowStoreType>(micro_header->row_store_type_)
      : MAX_ROW_STORE;
  }

  TO_STRING_KV(KP_(buf), K_(size), KP_(extra_buf), K_(extra_size), K_(type));

  const char *buf_;
  int64_t size_;
  const char *extra_buf_;
  int64_t extra_size_;
  Type type_;
};

class KwMicroBlock
{
public:
  KwMicroBlock()
    : range_(), data_(), payload_data_(), read_info_(nullptr)//, micro_index_info_(nullptr)
  {}

  inline bool is_valid() const
  {
    return range_.is_valid() && header_.is_valid() && data_.is_valid() && nullptr != read_info_;
      // && nullptr != micro_index_info_;
  }

  TO_STRING_KV(K_(range), K_(header), K_(data), K_(payload_data), KP_(read_info));//, KP_(micro_index_info));

  KwDatumRange range_;
  KwMicroBlockHeader header_;
  KwMicroBlockData data_;
  KwMicroBlockData payload_data_;
  const KwTableReadInfo *read_info_;
  // const ObMicroIndexInfo *micro_index_info_;
};

struct KwIMicroBlockReaderInfo
{
public:
  static const int64_t INVALID_ROW_INDEX = -1;
  KwIMicroBlockReaderInfo()
      : is_inited_(false),
        row_count_(-1),
        read_info_(nullptr)
  {}
  virtual ~KwIMicroBlockReaderInfo() { reset(); }
  inline int64_t row_count() const { return row_count_; }
  inline void reset()
  {
    row_count_ = -1;
    read_info_ = nullptr;
    is_inited_ = false;
  }

  bool is_inited_;
  int64_t row_count_;
  const KwTableReadInfo *read_info_;
};

class KwIMicroBlockGetReader : public KwIMicroBlockReaderInfo
{
public:
  KwIMicroBlockGetReader()
   : KwIMicroBlockReaderInfo()
  {
  }
  virtual ~KwIMicroBlockGetReader() {};
  // virtual int get_row(
  //     const KwMicroBlockData &block_data,
  //     const KwDatumRowkey &rowkey,
  //     const storage::ObTableReadInfo &read_info,
  //     KwDatumRow &row) = 0;
    virtual int get_row(
      const KwMicroBlockData &block_data,
      const KwDatumRowkey &rowkey,
      const KwTableReadInfo &read_info,
      KwDatumRow &row) = 0;
  // virtual int exist_row(
  //     const KwMicroBlockData &block_data,
  //     const KwDatumRowkey &rowkey,
  //     const storage::ObTableReadInfo &read_info,
  //     bool &exist,
  //     bool &found) = 0;
    virtual int exist_row(
      const KwMicroBlockData &block_data,
      const KwDatumRowkey &rowkey,
      const KwTableReadInfo &read_info,
      bool &exist,
      bool &found) = 0;
protected:
//   inline static int init_hash_index(
//       const KwMicroBlockData &block_data,
//       KwMicroBlockHashIndex &hash_index,
//       const ObMicroBlockHeader *header)
//   {
//     int ret = 0;
//     hash_index.reset();
//     if (header->is_contain_hash_index() && OB_FAIL(hash_index.init(block_data))) { ret = -1;
//     //   STORAGE_LOG(WARN, "failed to init micro block hash index", K(ret), K(block_data));
//     }
//     return ret;
//   }
};

class KwIMicroBlockReader : public KwIMicroBlockReaderInfo
{
public:
  enum KwReaderType
  {
    Reader,
    Decoder,
  };
  KwIMicroBlockReader()
    : KwIMicroBlockReaderInfo()
  {}
  virtual ~KwIMicroBlockReader() {}
  virtual KwReaderType get_type() = 0;
  virtual void reset() { KwIMicroBlockReaderInfo::reset(); }
  virtual int init(
      const KwMicroBlockData &block_data,
      const KwTableReadInfo &read_info) = 0;
  virtual int get_row(const int64_t index, KwDatumRow &row) = 0;
  virtual int get_row_header(
      const int64_t row_idx,
      const KwRowHeader *&row_header) = 0;
  virtual int get_row_count(int64_t &row_count) = 0;
  virtual int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const KwRowHeader *&row_header,
      int64_t &trans_version,
      int64_t &sql_sequence) = 0;
  int locate_range(
      const KwDatumRange &range,
      const bool is_left_border,
      const bool is_right_border,
      int64_t &begin_idx,
      int64_t &end_idx,
      const bool is_index_block = false);
  virtual int get_row_count(
      int32_t col_id,
      const int64_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count)
  {
    UNUSEDx(col_id, row_ids, row_cap, contains_null, count);
    return -4;//OB_NOT_SUPPORTED;
  }
  virtual int64_t get_column_count() const = 0;

protected:
  virtual int find_bound(
      // const KwDatumRowkey &key,
      const KwDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) = 0;
  virtual int find_bound(const KwDatumRange &range,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal,
      int64_t &end_key_begin_idx,
      int64_t &end_key_end_idx) = 0;
//   int validate_filter_info(
//       const sql::ObPushdownFilterExecutor &filter,
//       const void* col_buf,
//       const int64_t col_capacity,
//       const ObMicroBlockHeader *header);
//   int filter_white_filter(
//       const sql::ObWhiteFilterExecutor &filter,
//       const common::ObObj &obj,
//       bool &filtered);
};

} // namespace blocksstable
} // namespace kingwow
#endif

