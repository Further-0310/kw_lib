#define USING_LOG_PREFIX STORAGE
#include "kw_macro_block_reader.h"
#include "kw_sstable_printer.h"
#include "kw_micro_block_reader.h"
#include <iostream>

namespace oceanbase
{
namespace blocksstable
{
/*************************KwMacroBlockReader****************************/


/*************************KwSSTableDataBlockReader****************************/
KwSSTableDataBlockReader::KwSSTableDataBlockReader()
    :   data_(NULL), size_(0), pos_(0), common_header_(), macro_header_(),
        column_types_(nullptr), column_orders_(nullptr), column_checksum_(nullptr),
        read_info_(), micro_reader_(nullptr), allocator_(ObModIds::OB_CS_SSTABLE_READER),
        is_trans_sstable_(false), is_inited_(false)//, column_type_array_cnt_(0)
{}

KwSSTableDataBlockReader::~KwSSTableDataBlockReader() {}

int KwSSTableDataBlockReader::init(const char *data, const int64_t size)
{
    int ret = 0;
    int64_t pos = 0;
    if(is_inited_) { ret = OB_INIT_TWICE; }
    else if(NULL == data || size <= 0) { ret = OB_INVALID_ARGUMENT; }
    else if(0 > (ret = common_header_.deserialize(data, size, pos))) {}
    else if(0 > (ret = common_header_.check_integrity())) {}
    else {
        data_ = data;
        size_ = size;
        switch(common_header_.get_type())
        {
            case KwMacroBlockCommonHeader::SSTableData:
            case KwMacroBlockCommonHeader::SSTableIndex:
            {
                if(0 > (ret = macro_header_.deserialize(data, size, pos))) {}
                else if(pos != macro_header_.fixed_header_.micro_block_data_offset_) { ret = OB_ERR_UNEXPECTED; }
                else {
                    column_types_ = macro_header_.column_types_;
                    column_orders_ = macro_header_.column_orders_;
                    column_checksum_ = macro_header_.column_checksum_;
                    // column_type_array_cnt_ = macro_header_.fixed_header_.get_col_type_array_cnt();
                }
                break;
            }
            // case KwMacroBlockCommonHeader::LinkedBlock:
            // case KwMacroBlockCommonHeader::BloomFilterData:
            // case KwMacroBlockCommonHeader::SSTableMacroMeta:
            default:
            {
                ret = OB_NOT_SUPPORTED;
                LOG_ERROR("Not supported macro block type", K(ret), K_(common_header));
            }
        }
        // if (OB_SUCC(ret) && hex_print) {
        //     if (OB_ISNULL(hex_print_buf_ = static_cast<char *>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
        //         ret = OB_ALLOCATE_MEMORY_FAILED;
        //         LOG_WARN("Failed to alloc memory for hex print", K(ret));
        //     }
        // }
    }

    if(0 == ret) { is_inited_ = true; pos_ = pos; }
    if(!is_inited_) { reset(); }
    return ret;
}

void KwSSTableDataBlockReader::reset()
{
    data_ = NULL;
    size_ = 0;
    common_header_.reset();
    macro_header_.reset();
    // linked_header_.reset();
    // bloomfilter_header_ = NULL;
    column_types_ = NULL;
    column_orders_ = NULL;
    column_checksum_ = NULL;
    // hex_print_buf_ = nullptr;
    allocator_.reset();
    is_inited_ = false;
}

int KwSSTableDataBlockReader::read_data_block()
{
    int ret = 0;
    KwSSTablePrinter::print_common_header(&common_header_);
    KwSSTablePrinter::print_macro_block_header(&macro_header_);
    if(0 > (ret = print_column_info(macro_header_.fixed_header_.column_count_))) {}
    else if(0 > (ret = print_sstable_macro_block())) {}
    return ret;
}

int KwSSTableDataBlockReader::print_column_info(const int64_t col_cnt)
{
    int ret = 0;
    if(0 > col_cnt || NULL == column_types_ || NULL == column_orders_ || NULL == column_checksum_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid column info", K(ret), K(col_cnt), KP_(column_types), KP_(column_orders), KP_(column_checksum));
    }
    else if(col_cnt > 0) {
        KwSSTablePrinter::print_cols_info_start("column_index", "column_type", "column_order", "column_checksum", "collation_type");
        for(int64_t i = 0; i < col_cnt; ++i) {
            KwSSTablePrinter::print_cols_info_line(i, column_types_[i].get_type(), column_orders_[i],
                column_checksum_[i], column_types_[i].get_collation_type());
        }
        KwSSTablePrinter::print_end_line();
    }
    return ret;
}

int KwSSTableDataBlockReader::print_sstable_macro_block()
{
    int ret = 0;
    // init read_info and micro_reader
    const int64_t column_cnt = macro_header_.fixed_header_.column_count_;
    ObSEArray<ObColDesc, 16> columns;
    ObColDesc col_desc;
    for (int64_t i = 0; 0 == ret && i < column_cnt; ++i) {
      col_desc.col_type_ = column_types_[i];
      if (OB_FAIL(columns.push_back(col_desc))) {
        LOG_WARN("Fail to push col desc to columns", K(ret));
      }
    }
    if(0 > ret) {}
    else if(0 > (ret = row_.init(allocator_, column_cnt))) {
        LOG_WARN("Fail to init datum row", K(ret));
    }
    else if(0 > (ret = read_info_.init(allocator_,
                    column_cnt,// - 2,
                    macro_header_.fixed_header_.rowkey_column_count_,// - 2,
                    lib::is_oracle_mode(),
                    columns,
                    true))){
        LOG_WARN("Fail to init column read info", K(ret), K_(macro_header));
    }
    // else if(0 > (ret = build_micro_reader(static_cast<ObRowStoreType>(macro_header_.fixed_header_.row_store_type_)))) {
    //     LOG_WARN("Fail to init micro block reader", K(ret), K_(macro_header));
    // }
    else {
        int64_t micro_block_cnt = macro_header_.fixed_header_.micro_block_count_;
        for(int64_t micro_idx = 0; 0 == ret && micro_idx < micro_block_cnt; ++micro_idx) {
            ret = print_sstable_micro_block(micro_idx);
        }
    }
    return ret;
}

int KwSSTableDataBlockReader::print_sstable_micro_block(const int64_t micro_idx)
{
    int ret = 0;

    // print micro header
    KwSSTablePrinter::print_title("Micro Block", micro_idx, 1);
    int64_t micro_pos = 0;
    KwMicroBlockHeader micro_header;
    if(0 > (ret = micro_header.deserialize(data_ + pos_, size_ - pos_, micro_pos))) {
        LOG_WARN("Read an invalid micro block data", K(ret), K(micro_pos));
    }
    KwSSTablePrinter::print_micro_header(&micro_header);

    int64_t micro_data_size = micro_header.data_length_;
    KwMicroBlockData micro_data(data_ + pos_, micro_data_size);
    if(!micro_data.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Read an invalid micro block data", K(ret), K(micro_data_size));
    }
    else {
        switch (static_cast<ObRowStoreType>(macro_header_.fixed_header_.row_store_type_))
        {
        case FLAT_ROW_STORE:
        {
            KwMicroBlockReader micro_reader;
            if(0 > (ret = (micro_reader.init(micro_data, read_info_)))){
                LOG_WARN("Fail to init micro block reader", K(ret), K(micro_data), K_(read_info));
            }
            else {
                const KwDatumRow *print_row = nullptr;
                for(int64_t row_idx = 0; 0 == ret && row_idx < micro_header.row_count_; ++row_idx){
                    if(0 > (ret = micro_reader.get_row(row_idx, row_))) {
                        LOG_WARN("Fail to get current row", K(ret), K(row_idx), K_(read_info));
                    }
                    else {
                        print_row = &row_;
                        if(!is_trans_sstable_) { KwSSTablePrinter::print_row_title(print_row, row_idx); }
                        else { fprintf(stderr, "ROW[%ld]:", row_idx); }
                        // KwSSTablePrinter::print_store_row(print_row, column_types_, micro_header.rowkey_column_count_, false, true);
                        ObObj obj;
                        for(int64_t j = 0; j < print_row->get_column_count(); ++j) {
                            print_row->storage_datums_[j].to_obj_enhance(obj, column_types_[j]);
                            // print_row->storage_datums_[j].to_obj_enhance(obj, read_info_.cols_desc_.at(j).col_type_);
                            KwSSTablePrinter::print_cell(obj);
                            // KwSSTablePrinter::print_cell(print_row->storage_datums_[j]);
                        }
                        std::cout << std::endl;

                        // if (is_index_block) {}
                    }
                }
                pos_ += micro_data_size;
                KwSSTablePrinter::print_end_line();
                // if (nullptr != hex_print_buf_) {
                //     KwSSTablePrinter::print_hex_micro_block(*block_data, hex_print_buf_, OB_DEFAULT_MACRO_BLOCK_SIZE);
                // }
            }
            break;
        } 
        default:
            ret = OB_NOT_SUPPORTED;
        }
    }

    // else if(0 > (ret = micro_reader_->init(micro_data, read_info_))) {
    //     LOG_WARN("Fail to init micro block reader", K(ret), K(micro_data), K_(read_info));
    // }
    // else {
    //     const KwDatumRow *print_row = nullptr;
    //     for(int64_t row_idx = 0; 0 == ret && row_idx < micro_header.column_count_; ++row_idx){
    //         if(0 > (ret = micro_reader_->get_row(row_idx, row_))) {
    //             LOG_WARN("Fail to get current row", K(ret), K(row_idx), K_(read_info));
    //         }
    //         else {
    //             print_row = &row_;
    //             if(!is_trans_sstable_) { KwSSTablePrinter::print_row_title(print_row, row_idx); }
    //             else { fprintf(stderr, "ROW[%ld]:", row_idx); }
    //             KwSSTablePrinter::print_store_row(print_row, column_types_, micro_header.rowkey_column_count_, false, true);
    //             // ObObj obj;
    //             // for(int64_t j = 0; j < print_row->get_column_count(); ++j) {
    //             //     // print_row->storage_datums_[j].to_obj_enhance(obj, column_types[j]);
    //             //     print_row->storage_datums_[j].to_obj_enhance(obj, read_info_.cols_desc_.at(j).col_type_);
    //             //     KwSSTablePrinter::print_cell(obj);
    //             //     // KwSSTablePrinter::print_cell(print_row->storage_datums_[j]);
    //             // }
    //             // std::cout << std::endl;

    //             // if (is_index_block) {}
    //         }
    //         pos_ += micro_data_size;
    //         KwSSTablePrinter::print_end_line();
    //     }
    //     // if (nullptr != hex_print_buf_) {
    //     //     KwSSTablePrinter::print_hex_micro_block(*block_data, hex_print_buf_, OB_DEFAULT_MACRO_BLOCK_SIZE);
    //     // }
    // }
    
    return ret;
}

int KwSSTableDataBlockReader::build_micro_reader(const ObRowStoreType store_type)
{//借鉴macro_writer的build_micro_writer，不使用OB_NEWx，其实OB_NEWx的逻辑也差不多
    int ret = 0;
    void *buf = nullptr;
    // KwMicroBlockDecoder *encoding_decoder = nullptr;
    KwMicroBlockReader *flat_reader = nullptr;
    if(NULL != micro_reader_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Micro block reader should be null before init", K(ret));
    }
    else {
    switch (store_type)
    {
        case FLAT_ROW_STORE:
        {
            if (NULL == (buf = allocator_.alloc(sizeof(KwMicroBlockReader)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
            }
            else if(NULL == (flat_reader = new (buf) KwMicroBlockReader())) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "fail to new flat reader", K(ret));
            } // writer在这里直接init了，但是reader的init在后面
            else {
                micro_reader_ = flat_reader;
            }
            break;
        }
        default:
        {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Not supported row store type", K(ret), K(store_type));
        }
    }
    }

    if(0 == ret) {
        if (NULL != (flat_reader)) {
            flat_reader->~KwMicroBlockReader();
            allocator_.free(flat_reader);
            flat_reader = nullptr;
        }
        //后续有encoder的话也需要释放
    }

    return ret;
}

}
}