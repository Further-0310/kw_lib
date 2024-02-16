#ifndef KW_ROW_WRITER_H
#define KW_ROW_WRITER_H

#include "kw_datum_row.h"
#include "kw_block_sstable_struct.h"
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
// #include "share/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{

struct KwRowBuffer
{
  KwRowBuffer() : buf_(local_buffer_), buf_size_(INIT_ROW_BUFFER_SIZE), local_buffer_() {}
  ~KwRowBuffer() { reset(); }
  inline void reset();
  inline int extend_buf();
  inline char *get_buf() { return buf_; }
  inline int64_t get_buf_size() const { return buf_size_; }
  inline bool is_buf_extendable() const { return buf_size_ < MAX_ROW_BUFFER_SIZE; }
  TO_STRING_KV(KP_(buf), K_(buf_size), KP_(local_buffer));
private:
  static const int64_t INIT_ROW_BUFFER_SIZE = 4096;
  static const int64_t MAX_ROW_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
  char *buf_;
  int64_t buf_size_;
  char local_buffer_[INIT_ROW_BUFFER_SIZE];
};

class KwRowWriter
{
public:
  KwRowWriter();
  virtual ~KwRowWriter();
  int write(
      const int64_t rowkey_column_count,
      // const KwDatumRow &row,
      const KwDatumRow &row,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  int write(const int64_t rowkey_column_cnt, const KwDatumRow &datum_row, char *&buf, int64_t &len);
  // int write_rowkey(const common::ObStoreRowkey &rowkey, char *&buf, int64_t &len);
  // int write(
  //     const int64_t rowkey_cnt,
  //     const storage::ObStoreRow &row,
  //     const ObIArray<int64_t> *update_idx,
  //     char *&buf,
  //     int64_t &len);
  void reset();
private:
  template <int64_t MAX_CNT>
  struct KwBitArray
  {
    KwBitArray()
    {
      bit_array_ptrs_[0] = nullptr;
      bit_array_ptrs_[1] = reinterpret_cast<char *> (bit_array_byte1_);
      bit_array_ptrs_[2] = reinterpret_cast<char *> (bit_array_byte2_);
      bit_array_ptrs_[3] = reinterpret_cast<char *> (bit_array_byte4_);
    }
    inline void set_val(const int64_t idx, const int64_t offset)
    {
      bit_array_byte1_[idx] = static_cast<uint8_t>(offset);
      bit_array_byte2_[idx] = static_cast<uint16_t>(offset);
      bit_array_byte4_[idx] = static_cast<uint32_t>(offset);
    }
    inline uint32_t get_val(const int64_t idx)
    {
      // OB_ASSERT(idx < MAX_CNT);
      assert(idx < MAX_CNT);
      return bit_array_byte4_[idx];
    }
    inline char *get_bit_array_ptr(const KwColClusterInfoMask::BYTES_LEN type)
    {
      // OB_ASSERT(type > 0 && type < 4);
      assert(type > 0 && type  < 4);
      return reinterpret_cast<char *> (bit_array_ptrs_[type]);
    }
    uint32_t bit_array_byte4_[MAX_CNT];
    uint16_t bit_array_byte2_[MAX_CNT];
    uint8_t bit_array_byte1_[MAX_CNT];
    char *bit_array_ptrs_[4];
  };
  
  // int inner_write_row(
  //     const int64_t rowkey_column_count,
  //     const storage::ObStoreRow &row,
  //     const ObIArray<int64_t> *update_idx);
  // inline int write_oracle_timestamp(const common::ObOTimestampData &ot_data, const common:ObOTiamstampMetaAttrType otmat);
  int append_column(const common::ObObj &obj);
  int append_column(const KwStorageDatum &datum);
  // int append_8_bytes_column(const KwStorageDatum &datum);
  int init_common(char *buf, const int64_t buf_size, const int64_t pos);
  // int check_row_valid(
  //     const storage::ObStoreRow &row,
  //     const int64_t rowkey_column_count);
  int append_row_header(
      const uint8_t row_flag,
      const uint8_t multi_version_flag,
      const int64_t trans_id,
      const int64_t column_cnt,
      const int64_t rowkey_cnt);
  template <typename T>
  int inner_write_cells(
    const T *cells,
    const int64_t cell_cnt);
  // template <typename T>
  // int append_flat_cell_array(
  //     const T *cells,
  //     const int64_t offset_start_pos,
  //     const int64_t start_idx,
  //     const int64_t end_idx);
  // template <typename T>
  // int append_sparse_cell_array(
  //     const T *cells,
  //     const int64_t offset_start_pos,
  //     const int64_t start_idx,
  //     const int64_t end_idx);
  // template<typename T, typename R>
  // int append_row_and_index(
  //     const T *cells,
  //     const int64_t offset_start_pos,
  //     const int64_t start_idx,
  //     const int64_t end_idx,
  //     const bool is_spase_row,
  //     R &bytes_info);
  template<typename T>
  int build_cluster(
      const int64_t cell_cnt,
      const T *cells);
  template<typename T>
  int write_col_in_cluster(
      const T *cells,
      const int64_t cluster_idx,
      const int64_t start_col_idx,
      const int64_t end_col_idx);
  template <int64_t MAX_CNT>
  inline int append_array(
    KwBitArray<MAX_CNT> &bit_array,
    const int64_t count,
    // KwColClusterInfoMask::BYTES_LEN &type);
    KwColClusterInfoMask::BYTES_LEN &type);
  template<class T>
  inline int append(const T &value);
  // template<class T>
  // void append_with_no_check(const T &value);
  // inline int write_uint(const uint64_t value, const int64_t bytes);
  // inline int write_number(const common::number::ObNumber &number);
  // inline int write_char(
  //     const ObString &char_value,
  //     const int64_t max_length);
  static int get_uint_byte(const uint64_t uint_value, int64_t &bytes);
  int alloc_buf_and_init(const bool retry = false);
  // if return false: cell in [col_idx] is NOP
  inline bool check_col_exist(const int64_t col_idx);
  // inline int append_special_val_array(const int64_t cell_count);
  // TO_STRING_KV(KP_(buf), K_(buf_size), K_(start_pos), K_(pos), K_(column_index_count), K_(rowkey_column_cnt),
  //     K_(cluster_cnt), KPC_(row_header), K_(update_array_idx), KPC_(update_idx_array), K_(cluster_cnt));

private:
  static const int64_t USE_SPARSE_NOP_CNT_IN_CLUSTER = KwRowHeader::CLUSTER_COLUMN_CNT - KwColClusterInfoMask::MAX_SPARSE_COL_CNT;  // 29
  static const int64_t MAX_CLUSTER_CNT = common::OB_ROW_MAX_COLUMNS_COUNT / KwRowHeader::CLUSTER_COLUMN_CNT + 1;
  static const int64_t MAX_COLUMN_COUNT_IN_CLUSTER = KwRowHeader::USE_CLUSTER_COLUMN_COUNT; // independent rowkey cluster + less than a cluster column cnt
  template <typename T>
  void loop_cells(
      const T *cells,
      const int64_t col_cnt,
      int64_t &cluster_cnt,
      bool *output_sparse_row);
  int check_update_idx_array_valid(
      const int64_t rowkey_column_count,
      const ObIArray<int64_t> *update_idx);

private:
  char *buf_;
  int64_t buf_size_;
  int64_t start_pos_;
  int64_t pos_;
  // KwRowHeader *row_header_;
  KwRowHeader *row_header_;
  int64_t column_index_count_;
  int64_t rowkey_column_cnt_;
  const ObIArray<int64_t> *update_idx_array_;
  int64_t update_array_idx_;
  int64_t cluster_cnt_;
  KwRowBuffer row_buffer_; //4k buffer
  bool use_sparse_row_[MAX_CLUSTER_CNT]; 
  KwBitArray<MAX_COLUMN_COUNT_IN_CLUSTER> column_idx_;// for sparse row
  KwBitArray<MAX_CLUSTER_CNT> cluster_offset_;
  KwBitArray<MAX_COLUMN_COUNT_IN_CLUSTER> column_offset_;
  uint8_t special_vals_[MAX_COLUMN_COUNT_IN_CLUSTER];//1.5倍的cluster列数
  // DISALLOW_COPY_AND_ASSIGN(KwRowWriter);
};

inline void KwRowBuffer::reset()
{
  if(buf_ != local_buffer_){
    if(nullptr != buf_) { common::ob_free(buf_); }
    buf_ = local_buffer_;
    buf_size_ = INIT_ROW_BUFFER_SIZE;
  }
}

inline int KwRowBuffer::extend_buf()
{
  int ret = 0;//OB_SUCCESS;
  void *buf = nullptr;
  if(buf_size_ >= MAX_ROW_BUFFER_SIZE) { ret = -1; }//OB_BUF_NOT_ENOUGH
  else if(NULL == (buf_ = reinterpret_cast<char *>(common::ob_malloc(MAX_ROW_BUFFER_SIZE, "KwRowBuffer")))) { ret = -2; } // OB_ALLOCATE_MEMORY_FAILED
  else { buf_size_ = MAX_ROW_BUFFER_SIZE; }
  return ret;
}





} // end namespace blocksstable
} // end namespace kingwow
#endif
