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

#ifndef KW_SSTABLE_PRINTER_H_
#define KW_SSTABLE_PRINTER_H_

#include <stdint.h>
#include "kw_block_sstable_struct.h"
// #include "ob_index_block_row_struct.h"
// #include "ob_macro_block_common_header.h"
// #include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "kw_macro_block_common_header.h"
#include "kw_sstable_macro_block_header.h"
#include "kw_datum_row.h"
#include "kw_micro_block_header.h"

#define NONE_COLOR "\033[m"
#define RED "\033[0;32;31m"
#define LIGHT_RED "\033[1;31m"
#define GREEN "\033[0;32;32m"
#define LIGHT_GREEN "\033[1;32m"
#define BLUE "\033[0;32;34m"
#define LIGHT_BLUE "\033[1;34m"
#define DARY_GRAY "\033[1;30m"
#define CYAN "\033[0;36m"
#define LIGHT_CYAN "\033[1;36m"
#define PURPLE "\033[0;35m"
#define LIGHT_PURPLE "\033[1;35m"
#define BROWN "\033[0;33m"
#define YELLOW "\033[1;33m"
#define LIGHT_GRAY "\033[0;37m"
//#define WHITE "\033[1;37m"

namespace oceanbase
{
namespace blocksstable
{
class KwSSTablePrinter
{
public:
  static void print_title(const char *name, const int64_t value, const int64_t level = 1);
  static void print_title(const char *name, const int64_t level = 1);
  static void print_line(const char *name, const int32_t value, const int64_t level = 1);
  static void print_line(const char *name, const int64_t value, const int64_t level = 1);
  static void print_line(const char *name, const uint32_t value, const int64_t level = 1);
  static void print_line(const char *name, const uint64_t value, const int64_t level = 1);
  static void print_line(const char *name, const char *value, const int64_t level = 1);
  static void print_row_title(const blocksstable::KwDatumRow *row, const int64_t row_index);
  static void print_cell(const common::ObObj &cell);
  static void print_cell(const blocksstable::KwStorageDatum &datum);
  static void print_end_line(const int64_t level = 1);
  static void print_cols_info_start(const char *n1, const char *n2, const char *n3, const char *n4, const char *n5);
  static void print_cols_info_line(const int32_t &v1, const common::ObObjType v2, const common::ObOrderType v3, const int64_t &v4, const int64_t & v5);

  static void print_common_header(const KwMacroBlockCommonHeader *common_header);
  static void print_macro_block_header(const KwSSTableMacroBlockHeader *sstable_header);
  static void print_micro_header(const KwMicroBlockHeader *micro_block_header);
};

}
}

#endif /* KW_SSTABLE_PRINTER_H_ */
