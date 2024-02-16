#ifndef KW_STORAGE_BLOCKSSTABLE_KW_MICRO_BLOCK_READER_H_
#define KW_STORAGE_BLOCKSSTABLE_KW_MICRO_BLOCK_READER_H_

#include "kw_row_reader.h"
#include "kw_imicro_block_reader.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{

class KwIMicroBlockFlatReader
{
public:
  KwIMicroBlockFlatReader();
  virtual ~KwIMicroBlockFlatReader();
  void reset();
protected:
  int find_bound_(const KwDatumRowkey &key,
                         const bool lower_bound,
                         const int64_t begin_idx,
                         const int64_t end_idx,
                         const KwTableReadInfo &read_info,
                         int64_t &row_idx,
                         bool &equal);
  inline int init(const KwMicroBlockData &block_data);
protected:
  const KwMicroBlockHeader *header_;
  const char *data_begin_;
  const char *data_end_;
  const int32_t *index_data_;
  // TODO: remove allocator
  common::ObArenaAllocator allocator_;
  KwRowReader flat_row_reader_;
};


class KwMicroBlockReader : public KwIMicroBlockFlatReader, public KwIMicroBlockReader
{
public:
  KwMicroBlockReader()
    : KwIMicroBlockFlatReader(),
      KwIMicroBlockReader()
  {}
  virtual ~KwMicroBlockReader()
  { reset(); }
  virtual KwReaderType get_type() override { return Reader; }
  virtual void reset();
  virtual int init(
      const KwMicroBlockData &block_data,
      const KwTableReadInfo &read_info) override;
  virtual int get_row(const int64_t index, KwDatumRow &row) override;
  virtual int get_row_header(
      const int64_t row_idx,
      const KwRowHeader *&row_header) override;
  virtual int get_row_count(int64_t &row_count) override;
  int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const KwRowHeader *&row_header,
      int64_t &version,
      int64_t &sql_sequence);
  // Filter interface for filter pushdown
//   int filter_pushdown_filter(
//       const sql::ObPushdownFilterExecutor *parent,
//       sql::ObPushdownFilterExecutor &filter,
//       const storage::PushdownFilterInfo &pd_filter_info,
//       common::ObBitmap &result_bitmap);
  int get_rows(
    const common::ObIArray<int32_t> &cols_projector,
    const common::ObIArray<const ObColumnParam *> &col_params,
    const common::ObIArray<ObObjDatumMapType> &map_types, // TODO remove this, use datums directly
    const KwDatumRow &default_row,
    const int64_t *row_ids,
    const int64_t row_cap,
    KwDatumRow &row_buf,
    common::ObIArray<ObDatum *> &datums);//,
    // sql::ExprFixedArray &exprs,
    // sql::ObEvalCtx &eval_ctx);
  virtual int get_row_count(
      int32_t col,
      const int64_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count) override final;
  virtual int64_t get_column_count() const override
  {
    // OB_ASSERT(nullptr != header_);
    assert(nullptr != header_);
    return header_->column_count_;
  }
  // int get_min_or_max(
  //     int32_t col,
  //     const oceanbase::share::schema::ObColumnParam *col_param,
  //     const int64_t *row_ids,
  //     const int64_t row_cap,
  //     KwMicroBlockAggInfo<KwDatum> &agg_info);
  // int get_aggregate_result(
  //     const int64_t *row_ids,
  //     const int64_t row_cap,
  //     KwDatumRow &row_buf,
  //     oceanbase::common::ObIArray<storage::ObAggCell*> &agg_cells);
  inline bool single_version_rows() { return nullptr != header_ && header_->single_version_rows_; }

protected:
  virtual int find_bound(
      const KwDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) override;
  virtual int find_bound(
      const KwDatumRange &range,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal,
      int64_t &end_key_begin_idx,
      int64_t &end_key_end_idx) override;
};

class KwMicroBlockGetReader : public KwIMicroBlockFlatReader, public KwIMicroBlockGetReader
{
public:
  KwMicroBlockGetReader()
      : KwIMicroBlockFlatReader(),
        KwIMicroBlockGetReader()
        // hash_index_()
  {}
  virtual ~KwMicroBlockGetReader() {}
  virtual int get_row(
      const KwMicroBlockData &block_data,
      const KwDatumRowkey &rowkey,
      const KwTableReadInfo &read_info,
      KwDatumRow &row) final;
  // virtual int exist_row(
  //     const ObMicroBlockData &block_data,
  //     const KwDatumRowkey &rowkey,
  //     const ObTableReadInfo &read_info,
  //     bool &exist,
  //     bool &found) final;
  int locate_rowkey(const KwDatumRowkey &rowkey, int64_t &row_idx);
protected:
  int inner_init(const KwMicroBlockData &block_data,
                 const KwTableReadInfo &read_info,
                 const KwDatumRowkey &rowkey);
private:
  int locate_rowkey_fast_path(const KwDatumRowkey &rowkey,
                              int64_t &row_idx,
                              bool &need_binary_search,
                              bool &found);
private:
//   ObMicroBlockHashIndex hash_index_;
};

} // namespace blocksstable
} // namespace kingwow

#endif
