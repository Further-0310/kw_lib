#define USING_LOG_PREFIX STORAGE

#include "kw_macro_block_writer.h"
// #include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
/**
 * ---------------------------------------------------------KwMacroBlockWriter--------------------------------------------------------------
 */
KwMacroBlockWriter::KwMacroBlockWriter()
    :   data_store_desc_(nullptr),
        micro_writer_(nullptr),
        reader_helper_(),
        // hash_index_builder_(),
        // micro_helper_(),
        read_info_(),
        current_index_(0),
        current_macro_seq_(0),
        // block_write_ctx_(),
        // last_key_(),
        // last_key_with_L_flag_(false),
        is_macro_or_micro_block_reused_(false),
        curr_micro_column_checksum_(NULL),
        allocator_("MaBlkWriter", OB_MALLOC_NORMAL_BLOCK_SIZE),// MTL_ID()),
        rowkey_allocator_("MaBlkWriter", OB_MALLOC_NORMAL_BLOCK_SIZE)//, MTL_ID())//,
        // macro_reader_(),
        // micro_rowkey_hashs_(),
        // lock_(common::ObLatchIds::MACRO_WRITER_LOCK),
        // datum_row_(),
        // check_datum_row_(),
        // callback_(nullptr),
        // builder_(NULL),
        // data_block_pre_warmer_()
{} //macro_blocks_, macro_handles_

KwMacroBlockWriter::~KwMacroBlockWriter() { reset(); }

void KwMacroBlockWriter::reset()
{
    data_store_desc_ = nullptr;
    if (OB_NOT_NULL(micro_writer_)) {
        micro_writer_->~KwIMicroBlockWriter();
        allocator_.free(micro_writer_);
        micro_writer_ = nullptr;
    }
    reader_helper_.reset();
    // hash_index_builder_.reset();
    // micro_helper_.reset();
    read_info_.reset();
    macro_blocks_[0].reset();
    macro_blocks_[1].reset();
    // bf_cache_writer_[0].reset();
    // bf_cache_writer_[1].reset();
    current_index_ = 0;
    current_macro_seq_ = 0;
    // block_write_ctx_.reset();
    // macro_handles_[0].reset();
    // macro_handles_[1].reset();
    // last_key_.reset();
    // last_key_with_L_flag_ = false;
    is_macro_or_micro_block_reused_ = false;
    // micro_rowkey_hashs_.reset();
    // datum_row_.reset();
    // check_datum_row_.reset();
    // if (OB_NOT_NULL(builder_)) {
    //     builder_->~ObDataIndexBlockBuilder();
    //     builder_ = nullptr;
    // }
    // micro_block_adaptive_splitter_.reset();
    allocator_.reset();
    rowkey_allocator_.reset();
    // data_block_pre_warmer_.reset();
}

int KwMacroBlockWriter::open(KwDataStoreDesc &data_store_desc, const KwMacroDataSeq &start_seq)
{
    int ret = 0;
    reset();
    if(!data_store_desc.is_valid() || !start_seq.is_valid()) { ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid macro block writer input argument.", K(ret), K(data_store_desc), K(start_seq));}
    else if(0 > (ret = macro_blocks_[0].init(data_store_desc, start_seq.get_data_seq()))) {}
    else if(0 > (ret = macro_blocks_[1].init(data_store_desc, start_seq.get_data_seq()))) {}
    else {
        data_store_desc_ = &data_store_desc;
        current_macro_seq_ = start_seq.get_data_seq();
        if(0 > (ret = build_micro_writer(data_store_desc_, allocator_, micro_writer_, 2/*GCONF.micro_block_merge_verify_level*/))) {}
        else if(0 > (ret = read_info_.init(allocator_, data_store_desc.row_column_count_ - 2, data_store_desc.schema_rowkey_col_cnt_, lib::is_oracle_mode(), data_store_desc.col_desc_array_))) {}
        else if(0 > (ret = reader_helper_.init(allocator_))) {}

        if(0 == ret && data_store_desc_->is_major_merge()) {
            if(NULL == (curr_micro_column_checksum_ = static_cast<int64_t *>(
                allocator_.alloc(sizeof(int64_t) * data_store_desc_->row_column_count_)))) {
                ret = -1; // OB_ALLOCATE_MEMORY_FAILED
            }
            else {
                memset(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->rowkey_column_count_);
            }
        }
    }
    return ret;
}

int KwMacroBlockWriter::append_row(const KwDatumRow &row)
{
    int ret = 0;
    if(0 > (ret = append_row(row, data_store_desc_->micro_block_size_))) {}
    return ret;
}

int KwMacroBlockWriter::append_row(const KwDatumRow &row, const int64_t split_size)
{
    int ret = 0;
    const KwDatumRow *row_to_append = &row;
    bool is_need_set_micro_upper_bound = false;
    int64_t estimate_remain_size = 0;
    if(NULL == data_store_desc_) { ret = -1; /*OB_NOT_INIT*/}
    else if(split_size < data_store_desc_->micro_block_size_) { ret = -2; /*OB_INVALID_ARGUMENT*/}
    else if(0 > (ret = micro_writer_->append_row(row))) {
        if(ret != OB_BUF_NOT_ENOUGH) {
            STORAGE_LOG(WARN, "Failed to append row in micro writer", K(ret), K(row));
        }
    }
    return ret; 
}

int KwMacroBlockWriter::close()
{
    int ret = 0;//OB_SUCCESS;
    if (NULL == data_store_desc_ || NULL == micro_writer_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "exceptional situation", K(ret), K_(data_store_desc), K_(micro_writer));
    } else if (micro_writer_->get_row_count() > 0 && 0 > (ret = build_micro_block())) {
        STORAGE_LOG(WARN, "macro block writer fail to build current micro block.", K(ret));
    } else {
        KwMacroBlock &current_block = macro_blocks_[current_index_];
        if (0 == ret && current_block.is_dirty()) {
            int32_t row_count = current_block.get_row_count();
            if (0 > (ret = flush_macro_block(current_block))) {
                STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret),
                    K_(current_index));
            }
        }
    }
    return ret;
}

int KwMacroBlockWriter::build_micro_writer(KwDataStoreDesc *data_store_desc,
    ObIAllocator &allocator, KwIMicroBlockWriter *&micro_writer, const int64_t verify_level)
{
    int ret = 0;//OB_SUCCESS;
    void *buf = nullptr;
    // KwMicroBlockEncoder *encoding_writer = nullptr;
    KwMicroBlockWriter *flat_writer = nullptr;
    const bool need_calc_column_chksum = data_store_desc->is_major_merge();
    // 先不考虑encoding
    if (NULL == (buf = allocator.alloc(sizeof(KwMicroBlockWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } 
    else if (NULL == (flat_writer = new (buf) KwMicroBlockWriter())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to new encoding writer", K(ret));
    } 
    else if (0 > (ret = flat_writer->init(
            data_store_desc->micro_block_size_limit_,
            data_store_desc->rowkey_column_count_,
            data_store_desc->row_column_count_,
            &data_store_desc->col_desc_array_,
            need_calc_column_chksum))) {
        STORAGE_LOG(WARN, "Fail to init micro block flat writer, ", K(ret));
    } 
    else {
        flat_writer->set_micro_block_merge_verify_level(verify_level);
        micro_writer = flat_writer;
    }
    if (0 > ret) {
        // if (OB_NOT_NULL(encoding_writer)) {
        //     encoding_writer->~ObMicroBlockEncoder();
        //     allocator.free(encoding_writer);
        //     encoding_writer = nullptr;
        // }
        if (NULL != (flat_writer)) {
            flat_writer->~KwMicroBlockWriter();
            allocator.free(flat_writer);
            flat_writer = nullptr;
        }
    }
    return ret;
}

// int KwMacroBlockWriter::check_order(const KwDatumRow &row)
// {
//     int ret = 0;//OB_SUCCESS;
//     const int64_t trans_version_col_idx =
//         ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
//             data_store_desc_->schema_rowkey_col_cnt_,
//             data_store_desc_->rowkey_column_count_ != data_store_desc_->schema_rowkey_col_cnt_);
//     const int64_t sql_sequence_col_idx =
//         ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
//             data_store_desc_->schema_rowkey_col_cnt_,
//             data_store_desc_->rowkey_column_count_ != data_store_desc_->schema_rowkey_col_cnt_);
//     int64_t cur_row_version = 0;
//     int64_t cur_sql_sequence = 0;
//     if (!row.is_valid() || row.get_column_count() != data_store_desc_->row_column_count_) {
//         ret = OB_INVALID_ARGUMENT;
//         STORAGE_LOG(ERROR, "invalid macro block writer input argument.",
//             K(row), "row_column_count", data_store_desc_->row_column_count_, K(ret));
//     } else {
//         KwMacroBlock &curr_block = macro_blocks_[current_index_];
//         cur_row_version = row.storage_datums_[trans_version_col_idx].get_int();
//         cur_sql_sequence = row.storage_datums_[sql_sequence_col_idx].get_int();
//         if (cur_row_version >= 0 || cur_sql_sequence > 0) {
//             bool is_ghost_row_flag = false;
//             if (0 > (ret = blocksstable::KwGhostRowUtil::is_ghost_row(row.mvcc_row_flag_, is_ghost_row_flag))) {
//                 STORAGE_LOG(ERROR, "failed to check ghost row", K(ret), K(row));
//             } 
//             else if (!is_ghost_row_flag) {
//                 ret = OB_ERR_SYS;
//                 STORAGE_LOG(ERROR, "invalid trans_version or sql_sequence", K(ret), K(row), K(trans_version_col_idx),
//                             K(sql_sequence_col_idx));
//             }
//         }
//         else if (!row.mvcc_row_flag_.is_uncommitted_row()) { // update max commit version
//             micro_writer_->update_max_merged_trans_version(-cur_row_version);
//             if (!row.mvcc_row_flag_.is_shadow_row()) {
//                 const_cast<KwDatumRow&>(row).storage_datums_[sql_sequence_col_idx].reuse(); // make sql sequence positive
//                 const_cast<KwDatumRow&>(row).storage_datums_[sql_sequence_col_idx].set_int(0); // make sql sequence positive
//             } else if (OB_UNLIKELY(row.storage_datums_[sql_sequence_col_idx].get_int() != -INT64_MAX)) {
//                 ret = OB_ERR_UNEXPECTED;
//                 LOG_WARN("Unexpected shadow row", K(ret), K(row));
//             }
//         } 
//         else { // not committed
//             micro_writer_->set_contain_uncommitted_row();
//             LOG_TRACE("meet uncommited trans row", K(row));
//         }
//         // 关于rowkey的逻辑，简单来说就是比较当前的last_rowkey和新添加的row的rowkey
//         // 当前的last_rowkey不能超过新添加的row的rowkey
//     }
//     return ret;
// }

int KwMacroBlockWriter::build_micro_block()
{
    int ret = 0;//OB_SUCCESS;
    int tmp_ret = 0;//OB_SUCCESS;
    int64_t block_size = 0;
    KwMicroBlockDesc micro_block_desc;
    if (micro_writer_->get_row_count() <= 0) {
        ret = OB_INNER_STAT_ERROR;
        STORAGE_LOG(WARN, "micro_block_writer is empty", K(ret));
    } else if (0 > (ret = micro_writer_->build_micro_block_desc(micro_block_desc))) {
        STORAGE_LOG(WARN, "failed to build micro block desc", K(ret));
    } else {
        // micro_block_desc.last_rowkey_ = last_key_;
        block_size = micro_block_desc.buf_size_;
        if (0 > (ret = write_micro_block(micro_block_desc))) {
            STORAGE_LOG(WARN, "fail to write micro block ", K(ret), K(micro_block_desc));
        } 
    }

#ifdef ERRSIM
  if (data_store_desc_->encoding_enabled()) {
    ret = OB_E(EventTable::EN_BUILD_DATA_MICRO_BLOCK) ret;
  }
#endif

    if (0 == ret) {
        micro_writer_->reuse();
    }
    STORAGE_LOG(DEBUG, "build micro block desc", K(data_store_desc_->tablet_id_), K(micro_block_desc), "lbt", lbt(),
                                                K(ret), K(tmp_ret));
    return ret;
}

int KwMacroBlockWriter::write_micro_block(KwMicroBlockDesc &micro_block_desc)
{
    int ret = 0;//OB_SUCCESS;
    int64_t data_offset = 0;
    // if (0 > (ret = alloc_block())) {
    //     STORAGE_LOG(WARN, "Fail to pre-alloc block", K(ret));
    // 不用ObDataIndexBlockBuilder *builder_
    // } else {
        // we use macro_block.write_micro_block() to judge whether the micro_block can be added
        if (0 > (ret = macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
            if (OB_BUF_NOT_ENOUGH == ret) {
                if (0 > (ret = try_switch_macro_block())) {
                    STORAGE_LOG(WARN, "Fail to switch macro block, ", K(ret));
                } 
                // else if (0 > (ret = alloc_block())) {
                //     STORAGE_LOG(WARN, "Fail to pre-alloc block", K(ret));
                // } 
                else if (0 > (ret = macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
                    STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret));
                }
            } else {
                STORAGE_LOG(ERROR, "Fail to write micro block, ", K(ret), K(micro_block_desc));
            }
        }
        if (0 == ret) {
            // micro_block_desc.macro_id_ = macro_handles_[current_index_].get_macro_id();
            micro_block_desc.block_offset_ = data_offset;
        }
    // }

    // TODO(zhuixin.gsy): ensure bloomfilter correct for index micro block

    return ret;
}

int KwMacroBlockWriter::alloc_block()
{
    int ret = 0;//OB_SUCCESS;
    // ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
    // if (macro_blocks_[current_index_].is_dirty()) { // has been allocated
    // } else if (macro_handle.is_valid()) {
    //     STORAGE_LOG(INFO, "block maybe wrong", K(macro_handle));
    // } else if (0 > (ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_handle))) {
    //     STORAGE_LOG(WARN, "Fail to pre-alloc block for new macro block",
    //         K(ret), K_(current_index), K_(current_macro_seq));
    // }
    return ret;
}

int KwMacroBlockWriter::try_switch_macro_block()
{
    int ret = 0;//OB_SUCCESS;
    KwMacroBlock &macro_block = macro_blocks_[current_index_];
    if (macro_block.is_dirty()) {
        int32_t row_count = macro_block.get_row_count();
        if (0 > (ret = flush_macro_block(macro_block))) {
            STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret), K_(current_index));
        } else {
            current_index_ = (current_index_ + 1) % 2;
        }
    }
    // do not pre-alloc macro_id here, may occupy extra macro block
    return ret;
}

int KwMacroBlockWriter::flush_macro_block(KwMacroBlock &macro_block)
{
    int ret = 0;//OB_SUCCESS;

    // ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
    // ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];

    if (!macro_block.is_dirty()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "empty macro block has no pre-alloc macro id", K(ret), K(current_index_));;
    // } else if (0 > (ret = wait_io_finish(prev_handle))) {
    //     STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
    } else if (0 > (ret = macro_block.flush(/*macro_handle, block_write_ctx_*/))) {
        STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
    }
    if (0 == ret) {
        ++current_macro_seq_;
        if (0 > (ret = macro_block.init(*data_store_desc_, current_macro_seq_ + 1))) {
            STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
        }
    }
    return ret;
}

// int KwMacroBlockWriter::wait_io_finish(ObMacroBlockHandle &macro_handle)
// {
//     // wait prev_handle io finish
//     int ret = 0;//OB_SUCCESS;
//     const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
//     if (0 > (ret = macro_handle.wait(io_timeout_ms))) {
//         STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret), K(io_timeout_ms));
//     } else {
//         if (!macro_handle.is_empty()) {
//             FLOG_INFO("wait io finish", K(macro_handle.get_macro_id()));
//             int64_t check_level = 0;
//             if (NULL == (micro_writer_)) {
//                 ret = OB_ERR_UNEXPECTED;
//                 STORAGE_LOG(WARN, "micro_writer is null", K(ret));
//             } 
//             else if (FALSE_IT(check_level = micro_writer_->get_micro_block_merge_verify_level())) {} 
//             else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE == check_level) {
//                 if (0 > (ret = check_write_complete(macro_handle.get_macro_id()))) {
//                     STORAGE_LOG(WARN, "fail to check io complete", K(ret));
//                 }
//             }
//         }
//         macro_handle.reset();
//     }
//     return ret;
// }

// int KwMacroBlockWriter::check_write_complete(const MacroBlockId &macro_block_id)
// {
//     int ret = 0;//OB_SUCCESS;

//     ObMacroBlockReadInfo read_info;
//     read_info.macro_block_id_ = macro_block_id;
//     read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
//     read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
//     const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
//     ObMacroBlockHandle read_handle;
//     if (0 > (ret = ObBlockManager::async_read_block(read_info, read_handle))) {
//         STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info));
//     } else if (0 > (ret = read_handle.wait(io_timeout_ms))) {
//         STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
//     } else if (0 > (ret = macro_block_checker_.check(
//         read_handle.get_buffer(),
//         read_handle.get_data_size(),
//         CHECK_LEVEL_PHYSICAL))) {
//         STORAGE_LOG(WARN, "fail to verity macro block", K(ret), K(macro_block_id));
//     }
//     return ret;
// }

} // namespace blocksstable
} // namespace oceanbase
