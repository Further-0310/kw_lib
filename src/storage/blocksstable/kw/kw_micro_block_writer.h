#ifndef KINGWOW_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_WRITER_H_
#define KINGWOW_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_WRITER_H_

#include "kw_row_writer.h"
#include "kw_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{
// memory
//  |- row data buffer
//        |- ObMicroBlockHeader
//        |- row data
//  |- row index buffer
//        |- ObRowIndex
//  |- row hash index builder(optional)
//
// build output
//  |- compressed data
//        |- ObMicroBlockHeader
//        |- row data
//        |- RowIndex
//        |- RowHashIndex(optional)
class KwMicroBlockWriter : public KwIMicroBlockWriter
{
  static const int64_t INDEX_ENTRY_SIZE = sizeof(int32_t);
  static const int64_t DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t DEFAULT_INDEX_BUFFER_SIZE = 2 * 1024;
  static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
public:
    KwMicroBlockWriter();
    virtual ~KwMicroBlockWriter();
    int init(const int64_t micro_block_size_limit,
      const int64_t rowkey_column_count,
      const int64_t column_count = 0,
      const common::ObIArray<ObColDesc> *col_desc_array = nullptr,
      const bool need_calc_column_chksum = false);
    int append_row(const KwDatumRow &row);
    int build_block(char *&buf, int64_t &size);
    virtual void reuse();
    
    virtual inline int64_t get_row_count() const override { return NULL == header_ ? 0 : header_->row_count_; }
    virtual inline int64_t get_block_size() const override { return get_data_size() + get_index_size(); }
    virtual inline int64_t get_column_count() const override {return column_count_; }
    virtual inline int64_t get_data_size() const override
    {
        int64_t data_size = data_buffer_.length();
        if (data_size == 0) { // lazy allocate
            data_size = get_data_base_offset();
        }
        return data_size;
    }
    virtual inline int64_t get_original_size() const override
    {
        int64_t original_size = 0;
        if (NULL != header_) {
            original_size = data_buffer_.pos() - header_->header_size_;
        }
        return original_size;
    }
    // virtual int append_hash_index(ObMicroBlockHashIndexBuilder& hash_index_builder);
    // virtual bool has_enough_space_for_hash_index(const int64_t hash_index_size) const;
    void reset();

private:
    int inner_init();
    inline int64_t get_index_size() const
    {
        int64_t index_size = index_buffer_.length();
        if (index_size == 0) { // lazy allocate
            index_size = get_index_base_offset();
        }
        return index_size;
    }
    inline int64_t get_future_block_size(const int64_t row_length) const
    { return get_data_size() + row_length + get_index_size() + sizeof(int32_t); }//INDEX_ENTRY_SIZE = sizeof(int32_t);
    // inline int64_t get_data_base_offset() const { return KwMicroBlockHeader::get_serialize_size(column_count_, need_calc_column_chksum_); }
    inline int64_t get_data_base_offset() const { return KwMicroBlockHeader::get_serialize_size(column_count_, need_calc_column_chksum_); }
    inline int64_t get_index_base_offset() const { return sizeof(int32_t); }

    int reserve_header(
      const int64_t column_count,
      const int64_t rowkey_column_count,
      const bool need_calc_column_chksum);
    int process_out_row_columns(const KwDatumRow &row);

private:
    int64_t micro_block_size_limit_;
    int64_t column_count_;
    KwRowWriter row_writer_;
    int64_t rowkey_column_count_;    
    KwSelfBufferWriter data_buffer_;
    KwSelfBufferWriter index_buffer_;  
    const common::ObIArray<ObColDesc> *col_desc_array_;
    bool need_calc_column_chksum_;
    bool is_inited_;
};



} // namespace blocksstale
} // namespace kingwow
#endif
