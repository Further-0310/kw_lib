#ifndef KW_MACRO_BLOCK_READER_H_
#define KW_MACRO_BLOCK_READER_H_

#include "kw_macro_block_common_header.h"
#include "kw_imicro_block_reader.h"
#include "kw_micro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{

// class KwMacroBlockReader
// {
// public:
//     KwMacroBlockReader();
//     virtual ~KwMacroBlockReader();

// private:

// };

class KwSSTableDataBlockReader
{
public:
    KwSSTableDataBlockReader(/* args */);
    virtual ~KwSSTableDataBlockReader();

    int init(const char *data, const int64_t size);
    void reset();
    // int dump(const uint64_t tablet_id, const int64_t scn);
    int read_data_block();
private:
    int print_column_info(const int64_t col_cnt);
    int print_sstable_macro_block();
    int print_sstable_micro_block(const int64_t micro_idx);
    int build_micro_reader(/*ObIAllocator &allocator, KwIMicroBlockReader *&micro_reader, */const ObRowStoreType store_type);
private:
    // raw data
    const char *data_;
    int64_t size_;
    int64_t pos_;
    KwMacroBlockCommonHeader common_header_;
    KwSSTableMacroBlockHeader macro_header_;
    // ObLinkedMacroBlockHeader linked_header_;
    // parsed objects
    // const ObBloomFilterMacroBlockHeader *bloomfilter_header_;
    const common::ObObjMeta *column_types_;
    const common::ObOrderType *column_orders_;
    const int64_t *column_checksum_;
    common::ObSEArray<ObColDesc, common::OB_DEFAULT_SE_ARRAY_COUNT> columns_;
    // facility objects
    // KwMacroBlockReader macro_reader_;
    KwTableReadInfo read_info_;
    KwIMicroBlockReader *micro_reader_;
    common::ObArenaAllocator allocator_;
    KwDatumRow row_;
    // char *hex_print_buf_;
    bool is_trans_sstable_;
    bool is_inited_;
    // int64_t column_type_array_cnt_;
};




}
}

#endif