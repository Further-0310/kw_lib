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

#define USING_LOG_PREFIX STORAGE
#include "kw_block_sstable_struct.h"
#include "kw_macro_block_common_header.h"
#include "lib/string/ob_string.h"
// #include "share/ob_errno.h"
// #include "share/ob_force_print_log.h"


namespace oceanbase
{
using namespace common;
namespace blocksstable
{
KwMacroBlockCommonHeader::KwMacroBlockCommonHeader()
{
  reset();
}

void KwMacroBlockCommonHeader::reset()
{
  header_size_ = (int32_t)get_serialize_size();
  version_ = MACRO_BLOCK_COMMON_HEADER_VERSION;
  magic_ = MACRO_BLOCK_COMMON_HEADER_MAGIC;
  attr_ = KwMacroBlockType::None;
  payload_size_ = 0;
  payload_checksum_ = 0;
}

void KwMacroBlockCommonHeader::set_attr(const int64_t seq)
{
  KwMacroDataSeq macro_seq(seq);
  switch (macro_seq.block_type_) {
    case KwMacroDataSeq::DATA_BLOCK:
      attr_ = KwMacroBlockType::SSTableData;
      break;
    case KwMacroDataSeq::INDEX_BLOCK:
      attr_ = KwMacroBlockType::SSTableIndex;
      break;
    case KwMacroDataSeq::META_BLOCK:
      attr_ = KwMacroBlockType::SSTableMacroMeta;
      break;
    default:
      attr_ = KwMacroBlockType::MaxMacroType;
      // LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid data seq", K(seq));
  }
}

int KwMacroBlockCommonHeader::build_serialized_header(char *buf, const int64_t len) const
{
  int ret = 0;//OB_SUCCESS;
  int64_t pos = 0;
  if (/*OB_FAIL*/0 > (serialize(buf, len, pos))) { ret = -1;
    // LOG_ERROR("fail to serialize record header, ", K(ret), KP(buf), K(len), K(pos), K(*this));
  } else if (get_serialize_size() != pos) {
    ret = -2;//OB_SERIALIZE_ERROR;
    // LOG_ERROR("serialize size mismatch, ", K(ret), K(pos), K(*this));
  }
  return ret;
}

int KwMacroBlockCommonHeader::check_integrity() const
{
  int ret = 0;//OB_SUCCESS;
  if (header_size_ != get_serialize_size()
      || version_ != MACRO_BLOCK_COMMON_HEADER_VERSION
      || magic_ != MACRO_BLOCK_COMMON_HEADER_MAGIC) {
    ret = -1;//OB_INVALID_DATA;
    // LOG_WARN("invalid common header", K(ret), K(*this));
  }
  return ret;
}

bool KwMacroBlockCommonHeader::is_valid() const
{
  bool b_ret = header_size_ > 0
      && version_ == MACRO_BLOCK_COMMON_HEADER_VERSION
      && MACRO_BLOCK_COMMON_HEADER_MAGIC == magic_
      && attr_ >= KwMacroBlockType::None
      && attr_ < KwMacroBlockType::MaxMacroType;
  return b_ret;
}

int KwMacroBlockCommonHeader::serialize(char *buf,
                                        const int64_t buf_len,
                                        int64_t& pos) const
{
  int ret = 0;//OB_SUCCESS;
  if (NULL == buf || buf_len < 0) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument.", K(ret), KP(buf), K(buf_len));
  } else if (pos + get_serialize_size() > buf_len) {
    ret = -2;//OB_BUF_NOT_ENOUGH;
    // LOG_ERROR("data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = -3;//OB_INVALID_ARGUMENT;
    // LOG_WARN("common header is invalid", K(ret), K(*this));
  } else {
    KwMacroBlockCommonHeader *common_header = reinterpret_cast<KwMacroBlockCommonHeader*>(buf + pos);
    common_header->header_size_ = header_size_;
    common_header->version_ = version_;
    common_header->magic_ = magic_;
    common_header->attr_ = attr_;
    common_header->payload_size_ = payload_size_;
    common_header->payload_checksum_ = payload_checksum_;
    pos += common_header->get_serialize_size();
  }
  return ret;
}

int KwMacroBlockCommonHeader::deserialize(const char *buf,
                                          const int64_t data_len,
                                          int64_t& pos)
{
  int ret = 0;//OB_SUCCESS;
  if (NULL == buf || data_len <= 0 || pos < 0) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // LOG_WARN("invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const KwMacroBlockCommonHeader *ptr = reinterpret_cast<const KwMacroBlockCommonHeader*>(buf + pos);
    header_size_ = ptr->header_size_;
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    attr_ = ptr->attr_;
    payload_size_ = ptr->payload_size_;
    payload_checksum_ = ptr->payload_checksum_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = -2;//OB_DESERIALIZE_ERROR;
      // LOG_ERROR("deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

int KwMacroBlockCommonHeader::get_attr_name(const int32_t attr, common::ObString &attr_name)
{
  int ret = 0;//OB_SUCCESS;
  attr_name = common::ObString("");
  switch(attr) {
    case None: {
      attr_name = common::ObString("None");
      break;
    }
    case SSTableData: {
      attr_name = common::ObString("SSTableData");
      break;
    }
    case LinkedBlock: {
      attr_name = common::ObString("LinkedBlock");
      break;
    }
    case TmpFileData: {
      attr_name = common::ObString("TmpFileData");
      break;
    }
    case SSTableMacroID: {
      attr_name = common::ObString("SSTableMacroID");
      break;
    }
    case BloomFilterData: {
      attr_name = common::ObString("BloomFilterData");
      break;
    }
    case SSTableIndex: {
      attr_name = common::ObString("SSTableIndex");
      break;
    }
    case SSTableMacroMeta: {
      attr_name = common::ObString("SSTableMacroMeta");
      break;
    }
    default: {
      ret = -1;//OB_INVALID_MACRO_BLOCK_TYPE;
      attr_name = common::ObString("Not Valid Type");
      // LOG_WARN("not valid macro block type", K(attr));
    }
  }
  return ret;
}
} // blocksstable
} // oceanbase
