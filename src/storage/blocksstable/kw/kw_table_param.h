#ifndef OB_TABLE_PARAM_H_
#define OB_TABLE_PARAM_H_

#include <stdint.h>
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace blocksstable
{

class ObColumnParam
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObColumnParam(common::ObIAllocator &allocator);
  virtual ~ObColumnParam();
  virtual void reset();
private:
  ObColumnParam();
  DISALLOW_COPY_AND_ASSIGN(ObColumnParam);

public:
  inline void set_column_id(const uint64_t column_id) { column_id_ = column_id; }
  inline void set_meta_type(const common::ObObjMeta meta_type) { meta_type_ = meta_type; }
  inline void set_column_order(const common::ObOrderType order) { order_ = order; }
  inline void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline int set_orig_default_value(const common::ObObj &default_value)
  { return deep_copy_obj(default_value, orig_default_value_); }
  inline int set_cur_default_value(const common::ObObj &default_value)
  { return deep_copy_obj(default_value, cur_default_value_); }
  int32_t get_data_length() const;
  inline uint64_t get_column_id() const { return column_id_; }
  inline const common::ObObjMeta &get_meta_type() const { return meta_type_; }
  inline common::ObOrderType get_column_order() const { return order_; }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  inline const common::ObObj &get_orig_default_value() const { return orig_default_value_; }
  inline const common::ObObj &get_cur_default_value() const { return cur_default_value_; }
  inline bool is_nullable_for_write() const { return is_nullable_for_write_; }
  inline void set_nullable_for_write(const bool nullable) { is_nullable_for_write_ = nullable; }
  inline bool is_nullable_for_read() const { return is_nullable_for_read_; }
  inline void set_nullable_for_read(const bool nullable) { is_nullable_for_read_ = nullable; }
  inline void set_gen_col_flag(const bool is_gen_col, const bool is_virtual)
  {
    is_gen_col_ = is_gen_col;
    is_virtual_gen_col_ = is_virtual;
  }
  inline bool is_gen_col() const { return is_gen_col_; }
  inline bool is_virtual_gen_col() const { return is_virtual_gen_col_; }
  inline bool is_gen_col_udf_expr() const { return is_gen_col_udf_expr_; }
  inline void set_gen_col_udf_expr(const bool flag) { is_gen_col_udf_expr_ = flag; }
  inline void set_is_hidden(const bool is_hidden) { is_hidden_ = is_hidden; }
  inline bool is_hidden() const { return is_hidden_; }
  int assign(const ObColumnParam &other);

  TO_STRING_KV(K_(column_id),
               K_(meta_type),
               K_(order),
               K_(accuracy),
               K_(orig_default_value),
               K_(cur_default_value),
               K_(is_nullable_for_write),
               K_(is_nullable_for_read),
               K_(is_gen_col),
               K_(is_virtual_gen_col),
               K_(is_gen_col_udf_expr),
               K_(is_hidden));
private:
  int deep_copy_obj(const common::ObObj &src, common::ObObj &dest);
private:
  common::ObIAllocator &allocator_;
  uint64_t column_id_;
  common::ObObjMeta meta_type_;
  common::ObOrderType order_;
  common::ObAccuracy accuracy_;
  common::ObObj orig_default_value_;
  common::ObObj cur_default_value_;
  bool is_nullable_for_write_;
  bool is_nullable_for_read_;
  bool is_gen_col_;
  bool is_virtual_gen_col_;
  bool is_gen_col_udf_expr_;
  bool is_hidden_;
};

struct ObColDesc final
{
public:
  ObColDesc():col_id_(0), col_type_(), col_order_(common::ObOrderType::ASC) {};
  ~ObColDesc() = default;
  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    (void)common::databuff_printf(buffer, length, pos, "column_id=%u ", col_id_);
    pos += col_type_.to_string(buffer + pos, length - pos);
    (void)common::databuff_printf(buffer, length, pos, " order=%d", col_order_);
    return pos;
  }
  int assign(const ObColDesc &other)
  {
    int ret = 0;
    if (this != &other) {
      col_id_ = other.col_id_;
      col_type_ = other.col_type_;
      col_order_ = other.col_order_;
    }
    return ret;
  }
  void reset();

  NEED_SERIALIZE_AND_DESERIALIZE;

  uint32_t col_id_;
  common::ObObjMeta col_type_;
  common::ObOrderType col_order_;
};
    
} // namespace blocksstable
} // namespace oceanbase


#endif