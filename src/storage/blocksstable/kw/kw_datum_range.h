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

#ifndef KW_STORAGE_BLOCKSSTABLE_DATUM_RANGE_H
#define KW_STORAGE_BLOCKSSTABLE_DATUM_RANGE_H

#include "kw_datum_rowkey.h"
#include "lib/utility/ob_print_kv.h"

namespace oceanbase
{
namespace blocksstable
{

static const int8_t INCLUSIVE_START = 0x1;
static const int8_t INCLUSIVE_END = 0x2;
static const int8_t MIN_VALUE = 0x4;
static const int8_t MAX_VALUE = 0x8;

struct KwDatumRange
{
public:
  KwDatumRange() : start_key_(), end_key_(), group_idx_(0), border_flag_(0) { }
  ~KwDatumRange() = default;
  inline void reset();
  inline bool is_valid() const;
  // inline bool is_memtable_valid() const;
  inline const KwDatumRowkey& get_start_key() const { return start_key_; }
  inline const KwDatumRowkey& get_end_key() const { return end_key_; }
  // inline const ObBorderFlag& get_border_flag() const { return border_flag_; }
  inline const int64_t& get_border_flag() const { return border_flag_; }
  inline int64_t get_group_idx() const { return group_idx_; }
  // inline void set_inclusive(ObBorderFlag flag) { border_flag_.set_inclusive(flag.get_data()); }
  inline void set_inclusive(int64_t flag) { border_flag_ &= MIN_VALUE + MAX_VALUE; border_flag_ += flag & (INCLUSIVE_START + INCLUSIVE_END); }
  // inline void set_border_flag(ObBorderFlag flag) { border_flag_ = flag; }
  inline void set_border_flag(int64_t flag) { border_flag_ = flag; }
  inline void set_start_key(const KwDatumRowkey &start_key) { start_key_ = start_key; }
  inline void set_end_key(const KwDatumRowkey &end_key) { end_key_ = end_key; }
  inline void set_group_idx(const int64_t group_idx) { group_idx_ = group_idx; }

  // inline bool is_left_open() const { return !border_flag_.inclusive_start(); }
  inline bool is_left_open() const { return !((border_flag_ & INCLUSIVE_START) == INCLUSIVE_START); }
  // inline bool is_left_closed() const { return border_flag_.inclusive_start(); }
  inline bool is_left_closed() const { return (border_flag_ & INCLUSIVE_START) == INCLUSIVE_START; }
  // inline bool is_right_open() const { return !border_flag_.inclusive_end(); }
  inline bool is_right_open() const { return !((border_flag_ & INCLUSIVE_END) == INCLUSIVE_END); }
  // inline bool is_right_closed() const { return border_flag_.inclusive_end(); }
  inline bool is_right_closed() const { return (border_flag_ & INCLUSIVE_END) == INCLUSIVE_END; }
  // inline void set_left_open() { border_flag_.unset_inclusive_start(); }
  inline void set_left_open() { border_flag_ &= (~INCLUSIVE_START); }
  // inline void set_left_closed() { border_flag_.set_inclusive_start(); }
  inline void set_left_closed() { border_flag_ |= INCLUSIVE_START; }
  // inline void set_right_open() { border_flag_.unset_inclusive_end(); }
  inline void set_right_open() { border_flag_ &= (~INCLUSIVE_END); }
  // inline void set_right_closed() { border_flag_.set_inclusive_end(); }
  inline void set_right_closed() { border_flag_ |= INCLUSIVE_END; }
  inline void set_whole_range();
  inline bool is_whole_range() const { return start_key_.is_min_rowkey() && end_key_.is_max_rowkey(); }
  inline int is_single_rowkey(const KwStorageDatumUtils &datum_utils, bool &is_single) const;
  inline int include_rowkey(const KwDatumRowkey &rowkey, const KwStorageDatumUtils &datum_utils, bool &include);
  inline void change_boundary(const KwDatumRowkey &rowkey, bool is_reverse);
  // inline int from_range(const common::ObStoreRange &range, ObIAllocator &allocator);
  // inline int from_range(const common::ObNewRange &range, ObIAllocator &allocator);
  // inline int to_store_range(const common::ObIArray<share::schema::ObColDesc> &col_descs,
  //                             common::ObIAllocator &allocator,
  //                             common::ObStoreRange &store_range) const;
  inline int to_multi_version_range(common::ObIAllocator &allocator, KwDatumRange &dest) const;
  // inline int prepare_memtable_readable(const common::ObIArray<share::schema::ObColDesc> &col_descs,
  //                                         common::ObIAllocator &allocator);
  // !!Attension only compare start key
  inline int compare(const KwDatumRange &rhs, const KwStorageDatumUtils &datum_utils, int &cmp_ret) const;
  // maybe we will need serialize
  // NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(start_key), K_(end_key), K_(group_idx), K_(border_flag));
public:
  KwDatumRowkey start_key_;
  KwDatumRowkey end_key_;
  int64_t group_idx_;
  //TODO maybe we should use a new border flag
  // common::ObBorderFlag border_flag_;
  int64_t border_flag_; //0不包含，1包含start，2包含end
};

template<typename T>
struct KwDatumComparor
{
  KwDatumComparor(
      const KwStorageDatumUtils &datum_utils,
      int &ret,
      bool reverse = false,
      bool lower_bound = true)
    : datum_utils_(datum_utils), ret_(ret), reverse_(reverse), lower_bound_(lower_bound)
  {}
  KwDatumComparor() = delete;
  ~KwDatumComparor() = default;
  inline bool operator()(const T &left, const T &right)
  {
    int &ret = ret_;
    bool bret = false;
    int cmp_ret = 0;
    if (/*OB_FAIL*/ 0 > (ret)) {
    } else if (lower_bound_ || reverse_) {
      if (/*OB_FAIL*/ 0 > (ret = left.compare(right, datum_utils_, cmp_ret))) {
        // STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
      } else {
        bret = reverse_ ? cmp_ret > 0 : cmp_ret < 0;
      }
    } else if (/*OB_FAIL*/ 0 > (ret = right.compare(left, datum_utils_, cmp_ret))) {
      // STORAGE_LOG(WARN, "Failed to compare datum rowkey or range", K(ret), K(left), K(right));
    } else {
      bret = cmp_ret > 0;
    }
    return bret;
  }
private:
  const KwStorageDatumUtils &datum_utils_;
  int &ret_;
  bool reverse_;
  bool lower_bound_;
};



inline void KwDatumRange::reset()
{
  start_key_.reset();
  end_key_.reset();
  group_idx_ = 0;
  // border_flag_.set_data(0);
  border_flag_ = 0;
}

//TODO without rowkey type, we cannot judge empty
inline bool KwDatumRange::is_valid() const
{
  return start_key_.is_valid() && end_key_.is_valid();
}

// inline bool KwDatumRange::is_memtable_valid() const
// {
//   return start_key_.is_memtable_valid() && end_key_.is_memtable_valid();
// }

inline void KwDatumRange::set_whole_range()
{
  start_key_.set_min_rowkey();
  end_key_.set_max_rowkey();
  group_idx_= 0;
  // border_flag_.set_all_open();
  border_flag_ = 0;
}

inline int KwDatumRange::is_single_rowkey(const KwStorageDatumUtils &datum_utils, bool &is_single) const
{
  int ret = 0;//OB_SUCCESS;

  is_single = false;
  // if (!border_flag_.inclusive_start() || !border_flag_.inclusive_end()) {
  if (is_left_open() || is_right_open()) {
  } else if (start_key_.is_ext_rowkey()) {
  } else if (/*OB_FAIL*/ 0 > (ret = start_key_.equal(end_key_, datum_utils, is_single))) {
    // STORAGE_LOG(WARN, "Failed to check datum rowkey equal", K(ret), K(*this));
  }

  return ret;
}

inline int KwDatumRange::include_rowkey(const KwDatumRowkey &rowkey,
                                           const KwStorageDatumUtils &datum_utils,
                                           bool &include)
{
  int ret = 0;//OB_SUCCESS;
  include = false;

  if (/*OB_UNLIKELY*/(!rowkey.is_valid() || !datum_utils.is_valid())) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to check include rowkey", K(ret), K(rowkey), K(datum_utils));
  } else {
    int cmp_ret = 0;
    if (/*OB_FAIL*/ 0 > (ret = start_key_.compare(rowkey, datum_utils, cmp_ret))) {
      // STORAGE_LOG(WARN, "Failed to do start key compare", K(ret), K(rowkey), K(*this));
    } else if (cmp_ret > 0 || (cmp_ret == 0 && is_left_open())) {
    } else if (/*OB_FAIL*/ 0 > (ret = end_key_.compare(rowkey, datum_utils, cmp_ret))) {
      // STORAGE_LOG(WARN, "Failed to do start key compare", K(ret), K(rowkey), K(*this));
    } else if (cmp_ret < 0 || (cmp_ret == 0 && is_right_open())) {
      include = true;
    }

  }

  return ret;
}

// inline int KwDatumRange::from_range(const common::ObNewRange &range, ObIAllocator &allocator)
// {
//   int ret = 0;//OB_SUCCESS;

//   //we should not defend the range valid
//   if (/*OB_UNLIKELY*/(!range.is_valid())) {
//     ret = -1;//OB_INVALID_ARGUMENT;
//     // STORAGE_LOG(WARN, "Invalid argument to ", K(ret), K(range));
//   } else if (/*OB_FAIL*/ 0 > (ret = start_key_.from_rowkey(range.get_start_key(), allocator))) {
//     // STORAGE_LOG(WARN, "Failed to from start key", K(ret));
//   } else if (/*OB_FAIL*/ 0 > (ret = end_key_.from_rowkey(range.get_end_key(), allocator))) {
//     // STORAGE_LOG(WARN, "Failed to from end key", K(ret));
//   } else {
//     border_flag_ = range.border_flag_;
//     group_idx_ = range.get_group_idx();
//   }

//   return ret;
// }

// inline int KwDatumRange::from_range(const common::ObStoreRange &range, ObIAllocator &allocator)
// {
//   ObNewRange new_range;
//   range.to_new_range(new_range);
//   return from_range(new_range, allocator);
// }

// inline int KwDatumRange::to_store_range(const common::ObIArray<share::schema::ObColDesc> &col_descs,
//                               common::ObIAllocator &allocator,
//                               common::ObStoreRange &store_range) const
// {
//   int ret = 0;//OB_SUCCESS;

//   store_range.reset();
//   if (/*OB_UNLIKELY*/(!is_valid())) {
//     ret = -1;//OB_INVALID_ARGUMENT;
//     // STORAGE_LOG(WARN, "Invalid argument to transfer to store range", K(ret), K(*this));
//   } else if (/*OB_FAIL*/ 0 > (ret = start_key_.to_store_rowkey(col_descs, allocator, store_range.get_start_key()))) {
//     // STORAGE_LOG(WARN, "Failed to transfer start key", K(ret), K(start_key_));
//   } else if (/*OB_FAIL*/ 0 > (ret = end_key_.to_store_rowkey(col_descs, allocator, store_range.get_end_key()))) {
//     // STORAGE_LOG(WARN, "Failed to transfer end key", K(ret), K(end_key_));
//   } else {
//     store_range.set_border_flag(border_flag_);
//     store_range.set_group_idx(group_idx_);
//   }

//   return ret;
// }

inline int KwDatumRange::compare(const KwDatumRange &rhs, const KwStorageDatumUtils &datum_utils, int &cmp_ret) const
{
  int ret = 0;//OB_SUCCESS;
  if (/*OB_UNLIKELY*/(!is_valid() || !rhs.is_valid())) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to compa datum range", K(ret), K(*this), K(rhs));
  } else {
    ret = start_key_.compare(rhs.get_start_key(), datum_utils, cmp_ret);
  }
  return ret;
}

inline void KwDatumRange::change_boundary(const KwDatumRowkey &rowkey, bool is_reverse)
{
  if (is_reverse) {
    end_key_ = rowkey;
    // border_flag_.unset_inclusive_end();
    set_right_open();
  }  else {
    start_key_ = rowkey;
    // border_flag_.unset_inclusive_start();
    set_left_open();
  }
}

inline int KwDatumRange::to_multi_version_range(common::ObIAllocator &allocator, KwDatumRange &dest) const
{
  int ret = 0;//OB_SUCCESS;
  const bool include_start = is_left_closed();//get_border_flag().inclusive_start();
  const bool include_end = is_right_closed();//get_border_flag().inclusive_end();

  if (/*OB_UNLIKELY*/(!is_valid())) {
    ret = -1;//OB_INVALID_ARGUMENT;
    // STORAGE_LOG(WARN, "Invalid argument to transfer multi version range", K(ret), K(*this));
  } else if (/*OB_FAIL*/ 0 > (ret = start_key_.to_multi_version_rowkey(include_start, allocator, dest.start_key_))) {
    // STORAGE_LOG(WARN, "Failed to transfer multi version rowkey", K(ret), K(include_start), K_(start_key));
  } else if (/*OB_FAIL*/ 0 > (ret = end_key_.to_multi_version_rowkey(!include_end, allocator, dest.end_key_))) {
    // STORAGE_LOG(WARN, "Failed to transfer multi version rowkey", K(ret), K(include_end), K_(end_key));
  } else {
    dest.border_flag_ = border_flag_;
    dest.group_idx_ = group_idx_;
  }

  return ret;
}

// inline int KwDatumRange::prepare_memtable_readable(const common::ObIArray<share::schema::ObColDesc> &col_descs,
//                                           common::ObIAllocator &allocator)
// {
//   int ret = 0;//OB_SUCCESS;
//   if (/*OB_FAIL*/ 0 > (ret = start_key_.prepare_memtable_readable(col_descs, allocator))) {
//     // STORAGE_LOG(WARN, "Failed to prepare start key", K(ret), K(start_key_), K(col_descs));
//   } else if (/*OB_FAIL*/ 0 > (ret = end_key_.prepare_memtable_readable(col_descs, allocator))) {
//     // STORAGE_LOG(WARN, "Failed to prepare end key", K(ret), K(end_key_), K(col_descs));
//   }
//   return ret;
// }

} // namespace blocksstable
} // namespace oceanbase
#endif
