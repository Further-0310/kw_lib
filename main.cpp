#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public

#include <iostream>  
#include "src/storage/blocksstable/kw/kw_macro_block_writer.h"
#include "src/storage/blocksstable/kw/kw_macro_block_reader.h"
// #include "src/storage/blocksstable/kw/kw_table_read_info.h"
#include "lib/number/ob_number_v2.h"

#define VARIABLE_BUF_LEN 128

#define SET_VALUE(obj_set_fun, type, value) \
{ \
    obj.obj_set_fun(static_cast<type>(value)); \
}

#define SET_NUMBER(allocator, obj_set_fun, value) \
{ \
  ObNumber number; \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%ld", value); \
  } \
  if(0 == (ret)){ \
    if(/*OB_SUCCESS*/0 != (ret = number.from(buf, *allocator))){ \
      /*STORAGE_LOG(WARN, "fail to format num", K(ret));*/ \
    } else { \
      obj.obj_set_fun(number); \
    } \
  } \
}

#define SET_CHAR(allocator, obj_set_fun, value) \
{ \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = -1;/*OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory");*/ \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%064ld", value); \
  } \
  if(/*OB_SUCC*/ 0 == (ret)){ \
    ObString str; \
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf))); \
    obj.obj_set_fun(str); \
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI); \
  } \
}

namespace oceanbase
{
using namespace number;

namespace blocksstable
{

class TestMain
{
public:
    static const int64_t rowkey_column_count = 2;
    static const int64_t column_num = 24;//ObHexStringType;
    static const int64_t macro_block_size = 2L * 1024 * 1024L;
    static const int64_t SNAPSHOT_VERSION = 2;
public:
    TestMain() : allocator_(ObModIds::TEST), p_allocator_(&allocator_), read_info_() {};
    int SetUp();
    int SetObj(const ObObjType &column_type, const uint64_t column_id, const int64_t seed, ObObj &obj);
    int GenerateRow(KwDatumRow row, const int64_t seed);
    int Write();
    // int Read();
    int RunTest();
protected:
    common::ObArenaAllocator allocator_;
    common::ObIAllocator *p_allocator_;
    KwTableReadInfo read_info_;
};

int TestMain::SetUp()
{
    int ret = 0;
    // p_allocator_ = &allocator_;
    ObArray<ObColDesc> columns;
    for(int64_t i = 0; 0 == ret && i < column_num; ++i) {
        ObObjType obj_type = static_cast<ObObjType>(i + 1);
        ObColDesc col_desc;
        common::ObObjMeta meta_type;
        meta_type.reset();
        meta_type.set_type(obj_type);
        meta_type.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
        col_desc.col_id_ = static_cast<int32_t>(i + 16);//OB_APP_MIN_COLUMN_ID
        col_desc.col_type_ = meta_type;
        // 只有rowkey的order才有意义
        // if(obj_type == common::ObIntType){
        //     col_desc.set_rowkey_position(1);
        // } else if (obj_type == common::ObNumberType){
        //     column.set_rowkey_position(2);
        // } else {
        //     column.set_rowkey_position(0);
        // }
        if(0 > (ret = columns.push_back(col_desc))) {
            std::cout << "Add column" << i << " failed" << std::endl;
        }
    }
    if(0 > ret) {
    } else if(0 > (ret = read_info_.init(allocator_, column_num, 2, lib::is_oracle_mode(), columns))) {
        std::cout << "Table Read Info init failed" << std::endl;
    } else {
        std::cout << "Set Up succeeded!" << std::endl;
    }
    return ret;
}

int TestMain::SetObj(const ObObjType &column_type, const uint64_t column_id, const int64_t seed, ObObj &obj)
{
    int ret = 0;
    // 设置值：自己列数 * 100 + 行数
    int64_t value = 100 * column_type + seed;
    switch (column_type)
    {
    case ObNullType:
        obj.set_null();
        break;
    case ObTinyIntType:
        SET_VALUE(set_tinyint, int8_t, value);
        break;
    case ObSmallIntType:
        SET_VALUE(set_smallint, int16_t, value);
        break;
    case ObMediumIntType:
        SET_VALUE(set_mediumint, int32_t, value);
        break; 
    case ObInt32Type:
        SET_VALUE(set_int32, int32_t, value);
        break;
    case ObIntType:
        SET_VALUE(set_int, int64_t, value);
        break;
    case ObUTinyIntType:
        SET_VALUE(set_utinyint, uint8_t, value);
        break;
    case ObUSmallIntType:
        SET_VALUE(set_usmallint, uint16_t, value);
        break;
    case ObUMediumIntType:
        SET_VALUE(set_umediumint, uint32_t, value);
        break;
    case ObUInt32Type:
        SET_VALUE(set_uint32, uint32_t, value);
        break;
    case ObUInt64Type:
        SET_VALUE(set_uint64, uint64_t, value);
        break;
    case ObFloatType:
        SET_VALUE(set_float, float, value);
        break;
    case ObUFloatType:
        SET_VALUE(set_ufloat, float, value);
        break;
    case ObDoubleType:
        SET_VALUE(set_double, double, value);
        break;
    case ObUDoubleType:
        SET_VALUE(set_udouble, double, value);
        break;
    case ObNumberType:
        SET_NUMBER(p_allocator_, set_number, value);
        break;
    case ObUNumberType:
        SET_NUMBER(p_allocator_, set_unumber, value);
        break;
    case ObDateType:
        SET_VALUE(set_date, int32_t, value);
        break;
    case ObDateTimeType:
        SET_VALUE(set_datetime, int64_t, value);
        break;
    case ObTimestampType:
        SET_VALUE(set_timestamp, int64_t, value);
        break;
    case ObTimeType:
        SET_VALUE(set_time, int64_t, value);
        break;
    case ObYearType:
        SET_VALUE(set_year, uint8_t, value);
        break;
    case ObVarcharType:
        SET_CHAR(p_allocator_, set_varchar, value);
        break;
    case ObCharType:
        SET_CHAR(p_allocator_, set_char, value);
        break;
    case ObRawType:
        SET_CHAR(p_allocator_, set_raw, value);
        break;
    case ObEnumInnerType:
        SET_CHAR(p_allocator_, set_enum_inner, value);
        break;
    case ObSetInnerType:
        SET_CHAR(p_allocator_, set_set_inner, value);
        break;
    case ObNVarchar2Type:
        SET_CHAR(p_allocator_, set_nvarchar2, value);
        break;
    case ObNCharType:
        SET_CHAR(p_allocator_, set_nchar, value);
        break;
    case ObHexStringType:{
        char *buf = NULL;
        if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))) { ret = -1; } // OB_ALLOCATE_MEMORY_FAILED
        // else if(rowkey_pos  > 0) { snprintf(buf, 128, "%0127ld", seed); } // not change this
        else { snprintf(buf, 128, "%0127ld", value); } // not change this
        if(0 == ret){
            ObString str;
            str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
            obj.set_hex_string(str);
        }
        //SET_CHAR(p_allocator_, rowkey_pos, set_hex_string, seed, value);
        break;
    }
    case ObExtendType:
        obj.set_nop_value(); //TODO just set ObActionFlag::OP_NOP
        break;
    case ObUnknownType:
        obj.set_unknown(seed);
        break;
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: 
    case ObJsonType:
    case ObGeometryType: {
        ObLobCommon *value = NULL;
        void *buf = NULL;
        if(NULL == (buf = p_allocator_->alloc(sizeof(ObLobCommon) + 10))) { ret = -5; } // OB_ALLOCATE_MEMORY_FAILED
        else {
            // ObLobIndex index;
            value = new (buf) ObLobCommon();
            int64_t byte_size = 10;
            if(column_type == ObTinyTextType) { byte_size = 2; }
            obj.meta_.set_collation_type(column_type == ObGeometryType ? CS_TYPE_BINARY : CS_TYPE_UTF8MB4_GENERAL_CI);
            obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
            obj.set_type(column_type);
            obj.set_lob_value(column_type, value, value->get_handle_size(byte_size));
        }
        break;
    }
    case ObBitType: {
        SET_VALUE(set_bit, uint64_t, value);
        break;
    }
    case ObEnumType: {
        SET_VALUE(set_enum, uint64_t, value);
        break;
    }
    case ObSetType: {
        SET_VALUE(set_set, uint64_t, value);
        break;
    }
    case ObTimestampTZType: {
        obj.set_otimestamp_value(ObTimestampTZType, value, static_cast<uint16_t>(12));
        break;
    }
    case ObTimestampLTZType: {
        obj.set_otimestamp_value(ObTimestampLTZType, value, static_cast<uint16_t>(12));
        break;
    }
    case ObTimestampNanoType: {
        obj.set_otimestamp_value(ObTimestampNanoType, value, static_cast<uint16_t>(12));
        break;
    }
    case ObIntervalYMType: {
        obj.set_interval_ym(ObIntervalYMValue(value));
        break;
    }
    case ObIntervalDSType: {
        obj.set_interval_ds(ObIntervalDSValue(value, 14));
        break;
    }
    case ObNumberFloatType: {
        uint32_t digits = value;
        obj.set_number_float(ObNumber(3, &digits));
        break;
    }
    // case ObURowIDType: {
    //     if (0 > (ret = generate_urowid_obj(rowkey_pos, seed, value, obj))) {
    //         // STORAGE_LOG(WARN, "fail to generate urowid obj");
    //     }
    //     break;
    // }
    default:
        //warn "not support this data type."
        ret = -8; //OB_NOT_SUPPORTED
    }
    return ret;
}

int TestMain::GenerateRow(KwDatumRow row, const int64_t seed)
{
    int ret = 0;
    if(row.count_ < read_info_.cols_desc_.count()) {
        ret = -1;
        std::cout << "Invalid arguments" << std::endl;
    }
    else {
        ObObj obj;
        for (int64_t i = 0; 0 == ret && i < read_info_.cols_desc_.count(); ++i) {
            const uint64_t column_id = read_info_.cols_desc_.at(i).col_id_;
            ObObjType column_type = read_info_.cols_desc_.at(i).col_type_.get_type();
            // ObCollationType column_collation_type = column_list_.at(i).col_type_.get_collation_type();
            if(0 > (ret = SetObj(column_type, column_id, seed, obj))) {} // warn "fail to set obj"
            else if(0 > (ret = row.storage_datums_[i].from_obj_enhance(obj))) {} //warn "Failed to transfer obj to datum"
        }
        row.row_flag_.set_flag(KwDmlFlag::KW_DF_INSERT);
        row.count_ = read_info_.cols_desc_.count();
    }
    return ret;
}

int TestMain::Write()
{
    int ret = 0;
    KwDatumRow row;
    KwMacroBlockWriter writer;
    KwMacroDataSeq start_seq(0);
    start_seq.set_data_block();
    KwDataStoreDesc desc;
    if(0 > (ret = row.init(allocator_, column_num))) {
        std::cout << "KwDatumRow init failed" << std::endl;
    }
    else if(0 > (ret = desc.init(read_info_, 1001, 50001, MINOR_MERGE))) {
        std::cout << "KwDataStoreDesc init failed" << std::endl;
    }
    else if(0 > (ret = writer.open(desc, start_seq))) {
        std::cout << "KwMacroBlockWriter open failed" << std::endl;
    }
    else {
        int64_t test_micro_cnt = 5;
        int64_t test_row_cnt = 10;
        for(int64_t i = 0; 0 == ret && i < test_micro_cnt; ++i) {
            for(int64_t j = 0; 0 == ret && j < test_row_cnt; ++j) {
                const int64_t seed = i * test_micro_cnt + j + 1;
                if(0 > (ret = GenerateRow(row, seed))) {
                    std::cout << "GenerateRow failed" << std::endl;
                }
                else if(0 > (ret = writer.append_row(row))) {
                    std::cout << "KwMacroBlockWriter write row failed" << std::endl;
                }
            }
            if(0 > (ret = writer.build_micro_block())) {
                std::cout << "KwMacroBlockWriter build micro block failed" << std::endl;
            }
        }
        if(0 > ret) {
        } else if(0 > (ret = writer.close())) {
            std::cout << "KwMacroBlockWriter close and flush failed" << std::endl;
        } else {
            std::cout << "Succeed to Write Block In Disk!" << std::endl;
        }
    }
    return ret;
}

int TestMain::RunTest()
{
    int ret = 0;
    if(0 > (ret = SetUp())) {}
    else if(0 > (ret = Write())) {}

    return ret;
}

}
}

int main() {  
    // 输出一些信息  
    std::cout << "Hello from main!" << std::endl;
    oceanbase::blocksstable::TestMain test;
    test.RunTest();
  
    return 0;  
}