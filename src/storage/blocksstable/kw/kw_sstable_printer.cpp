/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "kw_sstable_printer.h"
// #include "storage/tx/ob_tx_data_define.h"
// #include "storage/tx_table/ob_tx_table_iterator.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
#define FPRINTF(args...) fprintf(stderr, ##args)
#define P_BAR() FPRINTF("|")
#define P_DASH() FPRINTF("------------------------------")
#define P_END_DASH() FPRINTF("--------------------------------------------------------------------------------")
#define P_NAME(key) FPRINTF("%s", (key))
#define P_NAME_FMT(key) FPRINTF("%30s", (key))
#define P_VALUE_STR_B(key) FPRINTF("[%s]", (key))
#define P_VALUE_INT_B(key) FPRINTF("[%d]", (key))
#define P_VALUE_BINT_B(key) FPRINTF("[%ld]", (key))
#define P_VALUE_STR(key) FPRINTF("%s", (key))
#define P_VALUE_INT(key) FPRINTF("%d", (key))
#define P_VALUE_BINT(key) FPRINTF("%ld", (key))
#define P_NAME_BRACE(key) FPRINTF("{%s}", (key))
#define P_COLOR(color) FPRINTF(color)
#define P_LB() FPRINTF("[")
#define P_RB() FPRINTF("]")
#define P_LBRACE() FPRINTF("{")
#define P_RBRACE() FPRINTF("}")
#define P_COLON() FPRINTF(":")
#define P_COMMA() FPRINTF(",")
#define P_TAB() FPRINTF("\t")
#define P_LINE_NAME(key) FPRINTF("%30s", (key))
#define P_LINE_VALUE(key, type) P_LINE_VALUE_##type((key))
#define P_LINE_VALUE_INT(key) FPRINTF("%-30d", (key))
#define P_LINE_VALUE_BINT(key) FPRINTF("%-30ld", (key))
#define P_LINE_VALUE_STR(key) FPRINTF("%-30s", (key))
#define P_END() FPRINTF("\n")

#define P_TAB_LEVEL(level) \
  do { \
    for (int64_t i = 1; i < (level); ++i) { \
      P_TAB(); \
      if (i != level - 1) P_BAR(); \
    } \
  } while (0)

#define P_DUMP_LINE(type) \
  do { \
    P_TAB_LEVEL(level); \
    P_BAR(); \
    P_LINE_NAME(name); \
    P_BAR(); \
    P_LINE_VALUE(value, type); \
    P_END(); \
  } while (0)

#define P_DUMP_LINE_COLOR(type) \
  do { \
    P_COLOR(LIGHT_GREEN); \
    P_TAB_LEVEL(level); \
    P_BAR(); \
    P_COLOR(CYAN); \
    P_LINE_NAME(name); \
    P_BAR(); \
    P_COLOR(NONE_COLOR); \
    P_LINE_VALUE(value, type); \
    P_END(); \
  } while (0)

// mapping to ObObjType
static const char * OB_OBJ_TYPE_NAMES[ObMaxType] = {
    "ObNullType", "ObTinyIntType", "ObSmallIntType",
    "ObMediumIntType", "ObInt32Type", "ObIntType",
    "ObUTinyIntType", "ObUSmallIntType", "ObUMediumIntType",
    "ObUInt32Type", "ObUInt64Type", "ObFloatType",
    "ObDoubleType", "ObUFloatType", "ObUDoubleType",
    "ObNumberType", "ObUNumberType", "ObDateTimeType",
    "ObTimestampType", "ObDateType", "ObTimeType", "ObYearType",
    "ObVarcharType", "ObCharType", "ObHexStringType",
    "ObExtendType", "ObUnknowType", "ObTinyTextType",
    "ObTextType", "ObMediumTextType", "ObLongTextType", "ObBitType",
    "ObEnumType", "ObSetType", "ObEnumInnerType", "ObSetInnerType",
    "ObTimestampTZType", "ObTimestampLTZType", "ObTimestampNanoType",
    "ObRawType", "ObIntervalYMType", "ObIntervalDSType", "ObNumberFloatType",
    "ObNVarchar2Type", "ObNCharType", "ObURowIDType", "ObLobType",
    "ObJsonType", "ObGeometryType", "ObUserDefinedSQLType"
};

void KwSSTablePrinter::print_title(const char *title, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_NAME_BRACE(title);
  P_DASH();
  P_END();
}

void KwSSTablePrinter::print_end_line(const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_END_DASH();
  P_END();
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(NONE_COLOR);
  }
}

void KwSSTablePrinter::print_title(const char *title, const int64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  P_TAB_LEVEL(level);
  P_DASH();
  P_LBRACE();
  P_NAME(title);
  P_VALUE_BINT_B(value);
  P_RBRACE();
  P_DASH();
  P_END();
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(NONE_COLOR);
  }
}

void KwSSTablePrinter::print_line(const char *name, const uint32_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void KwSSTablePrinter::print_line(const char *name, const uint64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void KwSSTablePrinter::print_line(const char *name, const int32_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(INT);
  } else {
    P_DUMP_LINE(INT);
  }
}

void KwSSTablePrinter::print_line(const char *name, const int64_t value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(BINT);
  } else {
    P_DUMP_LINE(BINT);
  }
}

void KwSSTablePrinter::print_line(const char *name, const char *value, const int64_t level)
{
  if (isatty(fileno(stderr)) > 0) {
    P_DUMP_LINE_COLOR(STR);
  } else {
    P_DUMP_LINE(STR);
  }
}

void KwSSTablePrinter::print_cols_info_start(const char *n1, const char *n2, const char *n3, const char *n4, const char *n5)
{
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
  }
  FPRINTF("--------{%-15s %15s %15s %15s %15s}----------\n", n1, n2, n3, n4, n5);
}

void KwSSTablePrinter::print_cols_info_line(const int32_t &v1, const common::ObObjType v2, const common::ObOrderType v3, const int64_t &v4, const int64_t & v5)
{
  const char *v3_cstr = ObOrderType::ASC == v3 ? "ASC" : "DESC";
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
    P_BAR();
    P_COLOR(NONE_COLOR);
    FPRINTF("\t[%-15d %15s %15s %15ld %15ld]\n", v1, OB_OBJ_TYPE_NAMES[v2], v3_cstr, v4, v5);
  } else {
    P_BAR();
    FPRINTF("\t[%-15d %15s %15s %15ld %15ld]\n", v1, OB_OBJ_TYPE_NAMES[v2], v3_cstr, v4, v5);
  }
}

void KwSSTablePrinter::print_row_title(const KwDatumRow *row, const int64_t row_index)
{
  char dml_flag[16];
  char mvcc_flag[16];
  row->mvcc_row_flag_.format_str(mvcc_flag, 16);
  row->row_flag_.format_str(dml_flag, 16);
  if (isatty(fileno(stderr)) > 0) {
    P_COLOR(LIGHT_GREEN);
    P_BAR();
    P_COLOR(CYAN);
    P_NAME("ROW");
    P_VALUE_BINT_B(row_index);
    P_COLON();
    P_NAME("trans_id=");
    P_VALUE_STR_B(to_cstring(row->trans_id_));
    P_COMMA();
    P_NAME("dml_flag=");
    P_VALUE_STR_B(dml_flag);
    P_COMMA();
    P_NAME("mvcc_flag=");
    P_VALUE_STR_B(mvcc_flag);
    P_BAR();
    P_COLOR(NONE_COLOR);
  } else {
    P_BAR();
    P_NAME("ROW");
    P_VALUE_BINT_B(row_index);
    P_COLON();
    P_NAME("trans_id=");
    P_VALUE_STR_B(to_cstring(row->trans_id_));
    P_COMMA();
    P_NAME("dml_flag=");
    P_VALUE_STR_B(dml_flag);
    P_COMMA();
    P_NAME("mvcc_flag=");
    P_VALUE_STR_B(mvcc_flag);
    P_BAR();
  }
}

void KwSSTablePrinter::print_cell(const ObObj &cell)
{
  P_VALUE_STR_B(to_cstring(cell));
}

void KwSSTablePrinter::print_cell(const KwStorageDatum &datum)
{
  P_VALUE_STR_B(to_cstring(datum));
}

void KwSSTablePrinter::print_common_header(const KwMacroBlockCommonHeader *common_header)
{
  print_title("Common Header");
  print_line("header_size", common_header->get_header_size());
  print_line("version", common_header->get_version());
  print_line("magic", common_header->get_magic());
  print_line("attr", common_header->get_attr());
  print_line("payload_size", common_header->get_payload_size());
  print_line("payload_checksum", common_header->get_payload_checksum());
  print_end_line();
}

void KwSSTablePrinter::print_macro_block_header(const KwSSTableMacroBlockHeader *sstable_header)
{
  print_title("SSTable Macro Block Header");
  print_line("header_size", sstable_header->fixed_header_.header_size_);
  print_line("version", sstable_header->fixed_header_.version_);
  print_line("magic", sstable_header->fixed_header_.magic_);
  print_line("tablet_id", sstable_header->fixed_header_.tablet_id_);
  print_line("logical_version", sstable_header->fixed_header_.logical_version_);
  print_line("data_seq", sstable_header->fixed_header_.data_seq_);
  print_line("column_count", sstable_header->fixed_header_.column_count_);
  print_line("rowkey_column_count", sstable_header->fixed_header_.rowkey_column_count_);
  print_line("row_store_type", sstable_header->fixed_header_.row_store_type_);
  print_line("row_count", sstable_header->fixed_header_.row_count_);
  print_line("occupy_size", sstable_header->fixed_header_.occupy_size_);
  print_line("micro_block_count", sstable_header->fixed_header_.micro_block_count_);
  print_line("micro_block_data_offset", sstable_header->fixed_header_.micro_block_data_offset_);
  print_line("data_checksum", sstable_header->fixed_header_.data_checksum_);
  // print_line("compressor_type", sstable_header->fixed_header_.compressor_type_);
  print_line("master_key_id", sstable_header->fixed_header_.master_key_id_);
  print_end_line();
}

void KwSSTablePrinter::print_micro_header(const KwMicroBlockHeader *micro_block_header)
{
  print_title("Micro Header");
  print_line("header_size", micro_block_header->header_size_);
  print_line("version", micro_block_header->version_);
  print_line("magic", micro_block_header->magic_);
  print_line("column_count", micro_block_header->column_count_);
  print_line("rowkey_column_count", micro_block_header->rowkey_column_count_);
  print_line("row_count", micro_block_header->row_count_);
  print_line("row_store_type", micro_block_header->row_store_type_);
  print_line("opt", micro_block_header->opt_);
  print_line("row_offset", micro_block_header->row_offset_);
  print_line("data_length", micro_block_header->data_length_);
  print_line("data_zlength", micro_block_header->data_zlength_);
  print_line("data_checksum", micro_block_header->data_checksum_);
  if (micro_block_header->has_column_checksum_) {
    for (int64_t i = 0; i < micro_block_header->column_count_; ++i) {
      if (isatty(fileno(stderr)) > 0) {
        P_COLOR(LIGHT_GREEN);
        P_BAR();
        P_COLOR(NONE_COLOR);
        FPRINTF("column_chksum[%15ld]|%-30ld\n", i, micro_block_header->column_checksums_[i]);
      } else {
        P_BAR();
        FPRINTF("column_chksum[%15ld]|%-30ld\n", i, micro_block_header->column_checksums_[i]);
      }
    }
  }
  print_end_line();
}

}
}
