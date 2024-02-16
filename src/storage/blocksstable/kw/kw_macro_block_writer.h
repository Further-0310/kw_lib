#ifndef KINGWOW_STORAGE_BLOCKSSTABLE_MACRO_BLOCK_WRITER_H_
#define KINGWOW_STORAGE_BLOCKSSTABLE_MACRO_BLOCK_WRITER_H_


#include "kw_macro_block.h"
#include "kw_micro_block_writer.h"
#include "kw_micro_block_reader.h"
#include "kw_micro_block_reader_helper.h"
// #include "kw_macro_block_struct.h"
// #include "kw_macro_block_handle.h"
// #include "share/io/ob_io_manager.h"
// #include "share/schema/ob_table_schema.h"
// #include "lib/allocator/page_arena.h"
// #include "storage/blocksstable/ob_macro_block_checker.h"
// #include "kw_micro_block_encoder.h"

namespace oceanbase
{
namespace blocksstable
{
// macro block store struct
//  |- ObMacroBlockCommonHeader
//  |- ObSSTableMacroBlockHeader
//  |- column types
//  |- column orders
//  |- column checksum
//  |- MicroBlock 1
//  |- MicroBlock 2
//  |- MicroBlock N

class KwMacroBlockWriter
{
public:
    KwMacroBlockWriter();
    virtual ~KwMacroBlockWriter();
    virtual void reset();

    //MacroBlockWriter的初始化函数
    //主要用DataStoreDesc &data_store_desc初始化
    virtual int open(KwDataStoreDesc &data_store_desc,// const int64_t &start_seq);
        const KwMacroDataSeq &start_seq);//,
        // KwIMacroBlockFlushCallBack *callback = nullptr);// not yet
    // virtual int append_macro_block(const KwMacroBlockDesc &macro_desc);// not yet
    // virtual int append_micro_block(const KwMicroBlock &micro_block/*, const KwMacroBlockDesc *curr_macro_desc = nullptr*/);// not yet
    // 需要实现KwMicroBlock的ob_index_block_row_struct
    virtual int append_row(const KwDatumRow &row/*, const KwMacroBlockDesc *curr_macro_desc_ = nullptr*/);
    // int append_index_micro_block(KwMicroBlockDesc &micro_block_desc);// not yet
    // int check_data_macro_block_need_merge(const KwMacroBlockDesc &macro_desc, bool &need_merge);// not yet
    int close();// not yet
    // void dump_block_and_writer_buffer();// not yet
    // inline KwMacroBlocksWriteCtx &get_macro_block_ctx() { return block_write_ctx_; }// not yet
    inline int64_t get_last_macro_seq() const { return current_macro_seq_; } /* save our seq num */
    // TO_STRING_KV(K_(block_write_ctx));
    static int build_micro_writer(
        KwDataStoreDesc *data_store_desc,
        ObIAllocator &allocator,
        KwIMicroBlockWriter *&micro_writer,
        const int64_t verify_level = KW_MICRO_BLOCK_MERGE_VERIFY_LEVEL::KW_ENCODING_AND_COMPRESSION);
protected:
    virtual int build_micro_block();
    virtual int try_switch_macro_block();
    virtual bool is_keep_freespace() const { return false; }
    inline bool is_dirty() const { return macro_blocks_[current_index_].is_dirty() || 0 != micro_writer_->get_row_count(); }
    inline int64_t get_curr_micro_writer_row_count() const { return micro_writer_->get_row_count(); }
    inline int64_t get_macro_data_size() const { return macro_blocks_[current_index_].get_data_size() + micro_writer_->get_block_size(); }
private:
    int append_row(const KwDatumRow &row, const int64_t split_size);
    // int check_order(const KwDatumRow &row);
    // int init_hash_index_builder();
    // int append_row_and_hash_index(const KwDatumRow &row);
    // int build_micro_block_desc(
    //     const KwMicroBlock &micro_block,
    //     KwMicroBlockDesc &micro_block_desc,
    //     KwMicroBlockHeader &header_for_rewrite);
    // // int build_hash_index_block(KwMicroBlockDesc &micro_block_desc);
    // int build_micro_block_desc_with_rewrite(
    //     const KwMicroBlock &micro_block,
    //     KwMicroBlockDesc &micro_block_desc,
    //     KwMicroBlockHeader &header);
    // int build_micro_block_desc_with_reuse(
    //     const KwMicroBlock &micro_block,
    //     KwMicroBlockDesc &micro_block_desc);
    // 有关KwMicroBlock的都没有实现
    int write_micro_block(KwMicroBlockDesc &micro_block_desc);
    // int check_micro_block_need_merge(const KwMicroBlock &micro_block, bool &need_merge);
    // int merge_micro_block(const KwMicroBlock &micro_block);
    int flush_macro_block(KwMacroBlock &macro_block);
    // int wait_io_finish(ObMacroBlockHandle &macro_handle);
    int alloc_block();
    // int check_write_complete(const MacroBlockId &macro_block_id);
    // int save_last_key(const KwDatumRow &row);
    // int save_last_key(const KwDatumRowkey &last_key);
    // int add_row_checksum(const KwDatumRow &row);
    // int calc_micro_column_checksum(
    //     const int64_t column_cnt_,
    //     KwIMicroBlockReader &reader,
    //     int64_t *column_checksum);
    // int flush_reuse_macro_block(const KwDataMacroBlockMeta &macro_meta);
    // int open_bf_cache_writer(const KwDataStoreDesc &desc, const int64_t bloomfilter_size);
    // int flush_bf_to_cache(ObMacroBloomFilterCacheWriter &bf_cache_writer, const int32_t row_count);
    // void dump_micro_block(KwIMicroBlockWriter &mirco_writer);
    // void dump_macro_block(KwMacroBlock &macro_block);

public:
    static const int64_t DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD = 30;
private:
    static const int64_t DEFAULT_MACRO_BLOCK_COUNT = 128;
    // typedef common::ObSEArray<KwMacroBlockId, DEFAULT_MACRO_BLOCK_COUNT> MacroBlockList;
protected:
    KwDataStoreDesc *data_store_desc_; 
private:
    KwIMicroBlockWriter *micro_writer_; 
    KwMicroBlockReaderHelper reader_helper_; // 重写或归并时获取micro reader
    // ObMicroBlockHashIndexBuilder hash_index_builder_; // 构建hash索引
    // KwMicroBlockBufferHelper micro_helper_; // 压缩微块数据
    KwTableReadInfo read_info_;
    KwMacroBlock macro_blocks_[2];
    // ObMacroBloomFilterCacheWriter bf_cache_writer_[2];//associate with macro_blocks // 先不用
    int64_t current_index_;
    int64_t current_macro_seq_; // set by sstable layer
    // KwMacroBlocksWriteCtx block_write_ctx_; // 记录添加的macro id，作为macro block flush的参数
    // ObMacroBlockHandle macro_handles_[2]; // 处理宏块io
    // KwDatumRowkey last_key_; // 记录当前最后一行记录的rowkey
    // bool last_key_with_L_flag_;
    bool is_macro_or_micro_block_reused_;
    int64_t *curr_micro_column_checksum_;
    common::ObArenaAllocator allocator_;
    common::ObArenaAllocator rowkey_allocator_;
    // KwMacroBlockReader macro_reader_;
    // ObMacroBlockReader macro_reader_; // 主要用来解压数据
    // common::ObArray<uint32_t> micro_rowkey_hashs_; // 构建微块时判断是否需要bloom filter
    // ObSSTableMacroBlockChecker macro_block_checker_; // 检查是否写入完毕
    // common::SpinRWLock lock_; // 似乎没什么用
    // KwDatumRow datum_row_; // merge以及计算列校验和时会读取一个row
    // KwDatumRow check_datum_row_; // 似乎没什么用
    // KwIMacroBlockFlushCallBack *callback_; // close的时候要等待回调？
    // ObDataIndexBlockBuilder *builder_;  // 添加宏块刚需、添加索引微块刚需，也可用在添加微块中
    // KwMicroBlockAdaptiveSplitter micro_block_adaptive_splitter_; // 判断追加一行或构建微块时当前微块是否需要拆分
    // ObDataBlockCachePreWarmer data_block_pre_warmer_; // 构建微块时存放kvpair
};

} // namespace blocksstable
} // namespace oceanbase


#endif