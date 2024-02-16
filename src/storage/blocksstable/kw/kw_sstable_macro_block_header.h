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

#ifndef KW_SSTABLE_MACRO_BLOCK_HEADER_H_
#define KW_SSTABLE_MACRO_BLOCK_HEADER_H_

#include "kw_macro_block_common_header.h"
#include "common/object/ob_object.h"
// #include "share/ob_encryption_util.h"
// #include "lib/utility/ob_print_utils.h"
// #include "lib/compress/ob_compress_util.h"

namespace oceanbase
{
namespace blocksstable
{
class KwDataStoreDesc;

class KwSSTableMacroBlockHeader final
{
private:
  class FixedHeader final
  {
  public:
    FixedHeader();
    ~FixedHeader() = default;
    bool is_valid() const;
    void reset();
    int64_t get_col_type_array_cnt() const
    { return SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2 == version_ ? rowkey_column_count_ : column_count_; }
    TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(tablet_id), K_(logical_version),
        K_(data_seq), K_(column_count), K_(rowkey_column_count), K_(row_store_type), K_(row_count),
        K_(occupy_size), K_(micro_block_count), K_(micro_block_data_offset),K_(micro_block_data_size),
        K_(idx_block_offset), K_(idx_block_size), K_(meta_block_offset), K_(meta_block_size),
        K_(data_checksum), /*K_(compressor_type), */K_(encrypt_id),
        K_(master_key_id)/*, KPHEX_(encrypt_key, sizeof(encrypt_key_))*/);
  public:
    uint32_t header_size_;
    uint16_t version_;
    uint16_t magic_;
    uint64_t tablet_id_;
    int64_t logical_version_;
    int64_t data_seq_;
    int32_t column_count_;
    int32_t rowkey_column_count_;
    int32_t row_store_type_;
    int32_t row_count_;
    int32_t occupy_size_;
    int32_t micro_block_count_;
    int32_t micro_block_data_offset_;
    int32_t micro_block_data_size_;
    int32_t idx_block_offset_;
    int32_t idx_block_size_;
    int32_t meta_block_offset_;
    int32_t meta_block_size_;
    int64_t data_checksum_;
    int64_t encrypt_id_;
    int64_t master_key_id_;
    // ObCompressorType compressor_type_;
    // char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  };
public:
  KwSSTableMacroBlockHeader();
  ~KwSSTableMacroBlockHeader();
  bool is_valid() const;
  int init(
      const KwDataStoreDesc &desc,
      common::ObObjMeta *col_types,
      common::ObOrderType *col_orders,
      int64_t *col_checksum);
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  static int64_t get_fixed_header_size();
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  static const uint16_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_V1 = 1;
  static const uint16_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2 = 2; // only store rowkey type/order
  static const uint16_t SSTABLE_MACRO_BLOCK_HEADER_MAGIC = 1007;
  static int64_t get_variable_size_in_header(const int64_t column_cnt);
public:
  FixedHeader fixed_header_;
  common::ObObjMeta *column_types_;
  common::ObOrderType *column_orders_;
  int64_t *column_checksum_;
  bool is_inited_;
};

}//end namespace blocksstable
}//end namespace oceanbase

#endif /* OB_SSTABLE_MACRO_BLOCK_HEADER_H_ */
