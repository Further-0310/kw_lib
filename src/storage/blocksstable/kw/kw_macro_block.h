#ifndef KW_MACRO_BLOCK_H_
#define KW_MACRO_BLOCK_H_


#include "kw_block_sstable_struct.h"
#include "kw_data_buffer.h"
#include "kw_imicro_block_writer.h"
#include "kw_macro_block_common_header.h"
#include "kw_sstable_macro_block_header.h"
#include "kw_table_read_info.h"
// #include "storage/blocksstable/ob_sstable_meta.h"
// #include "lib/compress/ob_compress_util.h"
// #include "share/scn.h"
// #include "storage/blocksstable/ob_index_block_builder.h"
// #include "share/schema/ob_table_schema.h"
// #include "common/ob_store_format.h"
// #include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace blocksstable
{

struct KwDataStoreDesc
{
    static const int64_t DEFAULT_RESERVE_PERCENT = 90;
    static const int64_t MIN_MICRO_BLOCK_SIZE = 4 * 1024; //4KB
    static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
    static const int64_t MIN_SSTABLE_SNAPSHOT_VERSION = 1; // ref to ORIGIN_FOZEN_VERSION
    static const int64_t MIN_SSTABLE_END_LOG_TS = 1; // ref to ORIGIN_FOZEN_VERSION
    // static const ObCompressorType DEFAULT_MINOR_COMPRESSOR_TYPE = ObCompressorType::LZ4_COMPRESSOR;
    // emergency magic table id is 10000
    // static const uint64_t EMERGENCY_TENANT_ID_MAGIC = 0;
    // static const uint64_t EMERGENCY_LS_ID_MAGIC = 0;
    // static const ObTabletID EMERGENCY_TABLET_ID_MAGIC;
    // share::ObLSID ls_id_;
    int64_t ls_id_;
    // ObTabletID tablet_id_;
    int64_t tablet_id_;
    int64_t macro_block_size_;
    int64_t macro_store_size_; //macro_block_size_ * reserved_percent
    int64_t micro_block_size_;
    int64_t micro_block_size_limit_;
    int64_t row_column_count_;
    int64_t rowkey_column_count_;
    ObRowStoreType row_store_type_;
    bool need_build_hash_index_for_micro_block_;
    // int64_t schema_version_;
    int64_t schema_rowkey_col_cnt_;
    // KwMicroBlockEncoderOpt encoder_opt_;
    // ObSSTableMergeInfo *merge_info_;
    ObMergeType merge_type_;
    // need #include "storage/ob_sstable_struct.h"

    // ObSSTableIndexBuilder *sstable_index_builder_;
    // ObCompressorType compressor_type_;
    int64_t snapshot_version_;
    // share::SCN end_scn_;
    // int64_t progressive_merge_round_;
    int64_t encrypt_id_;
    bool need_prebuild_bloomfilter_;
    int64_t bloomfilter_rowkey_prefix_; // to be remove
    int64_t master_key_id_;
    // char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
    // indicate the min_cluster_version which trigger the major freeze
    // major_working_cluster_version_ == 0 means upgrade from old cluster
    // which still use freezeinfo without cluster version
    // int64_t major_working_cluster_version_;
    bool is_ddl_;
    bool need_pre_warm_;
    bool is_force_flat_store_type_;
    common::ObArenaAllocator allocator_;
    common::ObFixedArray<ObColDesc, common::ObIAllocator> col_desc_array_;
    // blocksstable::ObStorageDatumUtils datum_utils_;
    KwStorageDatumUtils datum_utils_;
    KwDataStoreDesc();
    ~KwDataStoreDesc();
    //ATTENSION!!! Only decreasing count of parameters is acceptable
    int init(const KwTableReadInfo &read_info,
            const int64_t &ls_id,//const share::ObLSID &ls_id,
            const int64_t tablet_id,//const common::ObTabletID tablet_id,
            const ObMergeType merge_type,
            const int64_t snapshot_version = MIN_SSTABLE_SNAPSHOT_VERSION,
            const int64_t cluster_version = 0);
    bool is_valid() const;
    void reset();
    int assign(const KwDataStoreDesc &desc);
    bool encoding_enabled() const { return ENCODING_ROW_STORE == row_store_type_ || SELECTIVE_ENCODING_ROW_STORE == row_store_type_; }
    void force_flat_store_type()
    {
        row_store_type_ = FLAT_ROW_STORE;
        is_force_flat_store_type_ = true;
    }
    bool is_store_type_valid() const;
    inline bool is_major_merge() const { return is_major_merge_type(merge_type_); }
    inline bool is_meta_major_merge() const { return is_meta_major_merge_type(merge_type_); }
    inline bool is_use_pct_free() const { return macro_block_size_ != macro_store_size_; }
    int64_t get_logical_version() const
    { return (is_major_merge() || is_meta_major_merge()) ? snapshot_version_ : 62;/*end_scn_.get_val_for_tx();*/ } // 默认是62
    TO_STRING_KV(
      K_(ls_id),
      K_(tablet_id),
      K_(micro_block_size),
      K_(micro_block_size_limit),
      K_(macro_block_size),
      K_(row_column_count),
      K_(rowkey_column_count),
      K_(schema_rowkey_col_cnt),
      K_(row_store_type),
    //   K_(encoder_opt),
      // K_(compressor_type),
      // K_(schema_version),
      // KP_(merge_info),
      K_(merge_type),
      K_(snapshot_version),
    //   K_(end_scn),
      K_(need_prebuild_bloomfilter),
      K_(bloomfilter_rowkey_prefix),
      K_(encrypt_id),
      K_(master_key_id),
    //   KPHEX_(encrypt_key, sizeof(encrypt_key_)),
    //   K_(major_working_cluster_version),
    //   KP_(sstable_index_builder),
      K_(is_ddl),
      K_(col_desc_array));

private:
  int cal_row_store_type(
      // const ObMergeSchema &schema,
      const ObMergeType merge_type);
//   int get_emergency_row_store_type();
  void fresh_col_meta();
};



class KwMacroBlock
{
public:
    KwMacroBlock();
    virtual ~KwMacroBlock();
    //宏块的初始化
    //用DataStoreDesc &spec初始化，计算宏块header的偏移量
    //宏块header包括MacroBlockCommonHeader、SSTableMacroBlockHeader、ObObjMeta、column orders、column checksum
    int init(KwDataStoreDesc &spec, const int64_t &cur_macro_seq);
    
    //把一个微块写入到当前宏块中
    //用MicroBlockDesc &micro_block_desc写入，复制序列化的微块数据到当前宏块的buffer中
    //1.检查传入的micro_block_desc，确保当前宏块buffer的剩余空间足够微块数据的buf大小和header大小
    //2.内部初始化，实际上就是检查当前宏块的data_buffer的情况
    //若data_为空即是首次写入，则需要通过当前宏块的DataStoreDesc来预先申请宏块大小的空间，以及初始化宏块header
    //**last_rowkey_相关暂时先不做了，后面需要再做
    //3.通过micro_block_desc获取微块数据的指针buf和大小，以及微块header
    //先序列化微块header，需要复制列校验和数组内容；然后复制序列化的数据;最后申请data_中header和data的空间，即更新current()指针
    //4.更新宏块header相关参数等
    int write_micro_block(const KwMicroBlockDesc &micro_block_desc, int64_t &data_offset);
    
    //把一个叶子索引微块写入宏块中
    //逻辑与上述write_micro_block大体相同，注意不能写入一个空的宏块中
    int write_index_micro_block(const KwMicroBlockDesc &micro_block_desc, const bool is_leaf_index_block, int64_t &data_offset);
    
    // int get_macro_block_meta(KwDataMacroBlockMeta &macro_meta);
    // int flush(ObMacroBlockHandle &macro_handle);//, ObMacroBlocksWriteCtx &block_write_ctx);
    int flush();
    void reset();
    void reuse();
    inline bool is_dirty() const { return is_dirty_; }
    int64_t get_data_size() const;
    int64_t get_remain_size() const;
    inline char *get_data_buf() { return data_.data(); }
    inline int32_t get_row_count() const { return macro_header_.fixed_header_.row_count_; }
    inline int32_t get_micro_block_count() const { return macro_header_.fixed_header_.micro_block_count_; }
    inline ObRowStoreType get_row_store_type() const { return static_cast<ObRowStoreType>(macro_header_.fixed_header_.row_store_type_); }
    void update_max_merged_trans_version(const int64_t max_merged_trans_version) 
    { 
        if(max_merged_trans_version > max_merged_trans_version_) {
            max_merged_trans_version_ = max_merged_trans_version;
        }
    }
    void set_contain_uncommitted_row() { contain_uncommitted_row_ = true; }
    static int64_t calc_basic_micro_block_data_offset(const uint64_t column_cnt);
private:
    int inner_init();
    int reserve_header(const KwDataStoreDesc &spec, const int64_t &cur_macro_seq);
    int check_micro_block(const KwMicroBlockDesc &micro_block_desc) const;
    int write_macro_header();
    int add_column_checksum(const int64_t *to_add_checksum, const int64_t column_cnt, int64_t *column_checksum);
private:
    inline const char *get_micro_block_data_ptr() const { return data_.data() + data_base_offset_; }
    inline int64_t get_micro_block_data_size() const { return data_.length() - data_base_offset_; }

private:
    KwDataStoreDesc *spec_;
    KwSelfBufferWriter data_; // micro header + data blocks;
    KwSSTableMacroBlockHeader macro_header_; // macro header store in head of data_;
    int64_t data_base_offset_;
    // KwDatumRowkey last_rowkey_;
    common::ObArenaAllocator rowkey_allocator_;
    bool is_dirty_;
    KwMacroBlockCommonHeader common_header_;
    int64_t max_merged_trans_version_;
    bool contain_uncommitted_row_;
    int64_t original_size_;
    int64_t data_size_;
    int64_t data_zsize_;
    int64_t cur_macro_seq_;
    bool is_inited_;
};


} // namespace blocksstable
} // namespace oceanbase


#endif /* KW_MACRO_BLOCK_H_ */