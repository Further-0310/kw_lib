#define USING_LOG_PREFIX SHARE_SCHEMA
#include "kw_table_param.h"

namespace oceanbase
{
namespace blocksstable
{

/*
**************ObColumnParam**********************
*/
ObColumnParam::ObColumnParam(ObIAllocator &allocator)
    : allocator_(allocator)
{
  reset();
}

ObColumnParam::~ObColumnParam()
{
}

void ObColumnParam::reset()
{
  column_id_ = OB_INVALID_ID;
  meta_type_.reset();
  order_ = ObOrderType::ASC;
  accuracy_.reset();
  orig_default_value_.reset();
  cur_default_value_.reset();
  is_nullable_for_write_ = false;
  is_nullable_for_read_ = false;
  is_gen_col_ = false;
  is_virtual_gen_col_ = false;
  is_gen_col_udf_expr_ = false;
  is_hidden_ = false;
}

int ObColumnParam::deep_copy_obj(const ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (size > 0) {
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    }
  } else {
    dest = src;
  }

  return ret;
}

int32_t ObColumnParam::get_data_length() const
{
  // return ObColumnSchemaV2::get_data_length(accuracy_, meta_type_);
  return (ob_is_accuracy_length_valid_tc(meta_type_.get_type()) ?
      accuracy_.get_length() : ob_obj_type_size(meta_type_.get_type()));
}

OB_DEF_SERIALIZE(ObColumnParam)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              column_id_,
              meta_type_,
              accuracy_,
              orig_default_value_,
              cur_default_value_,
              order_,
              is_nullable_for_write_,
              is_gen_col_,
              is_virtual_gen_col_,
              is_gen_col_udf_expr_,
              is_nullable_for_read_,
              is_hidden_);
  return ret;
}

OB_DEF_DESERIALIZE(ObColumnParam)
{
  int ret = OB_SUCCESS;
  ObObj orig_default_value;
  ObObj cur_default_value;

  LST_DO_CODE(OB_UNIS_DECODE,
              column_id_,
              meta_type_,
              accuracy_,
              orig_default_value,
              cur_default_value,
              order_);

  // compatibility code
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, is_nullable_for_write_))) {
        LOG_WARN("failed to decode index_schema_version_", K(ret));
      }
    } else {
      is_nullable_for_write_ = false;
    }
  }
  OB_UNIS_DECODE(is_gen_col_);
  OB_UNIS_DECODE(is_virtual_gen_col_);
  OB_UNIS_DECODE(is_gen_col_udf_expr_);
  OB_UNIS_DECODE(is_nullable_for_read_);
  OB_UNIS_DECODE(is_hidden_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_obj(orig_default_value, orig_default_value_))) {
      LOG_WARN("Fail to deep copy orig_default_value, ", K(ret), K_(orig_default_value));
    } else if (OB_FAIL(deep_copy_obj(cur_default_value, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K_(cur_default_value));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObColumnParam)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              column_id_,
              meta_type_,
              accuracy_,
              orig_default_value_,
              cur_default_value_,
              order_,
              is_nullable_for_write_,
              is_nullable_for_read_,
              is_gen_col_,
              is_virtual_gen_col_,
              is_gen_col_udf_expr_,
              is_hidden_);
  return len;
}

int ObColumnParam::assign(const ObColumnParam &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    column_id_ = other.column_id_;
    meta_type_ = other.meta_type_;
    order_ = other.order_;
    accuracy_ = other.accuracy_;
    is_nullable_for_write_ = other.is_nullable_for_write_;
    is_nullable_for_read_ = other.is_nullable_for_read_;
    is_gen_col_ = other.is_gen_col_;
    is_virtual_gen_col_ = other.is_virtual_gen_col_;
    is_gen_col_udf_expr_= other.is_gen_col_udf_expr_;
    is_hidden_ = other.is_hidden_;
    if (OB_FAIL(deep_copy_obj(other.cur_default_value_, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(cur_default_value_));
    } else if (OB_FAIL(deep_copy_obj(other.orig_default_value_, orig_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K(orig_default_value_));
    }
  }
  return ret;
}

void ObColDesc::reset()
{
  col_id_ = UINT32_MAX;//storage::ObStorageSchema::INVALID_ID;
  col_type_.reset();
  col_order_ = common::ObOrderType::ASC;
}

DEFINE_SERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(col_id_);
  OB_UNIS_ENCODE(col_type_);
  OB_UNIS_ENCODE(col_order_);
  return ret;
}

DEFINE_DESERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(col_id_);
  OB_UNIS_DECODE(col_type_);
  OB_UNIS_DECODE(col_order_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObColDesc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(col_id_);
  OB_UNIS_ADD_LEN(col_type_);
  OB_UNIS_ADD_LEN(col_order_);
  return len;
}


} // namespace blocksstable
} // namespace blocksstable
