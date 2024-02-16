#include "kw_imicro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{

/***********************KwIMicroBlockReader*****************************/
int KwIMicroBlockReader::locate_range(
    const KwDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    int64_t &begin_idx,
    int64_t &end_idx,
    const bool is_index_block)
{
  int ret = 0;//OB_SUCCESS;
  begin_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_idx = KwIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  bool equal = false;
  int64_t end_key_begin_idx = 0;
  int64_t end_key_end_idx = row_count_;
  if(0 > row_count_) { ret = -1; } // OB_ERR_UNEXPECTED
  else if(0 == row_count_) {}
  else {
    if(!is_left_border || range.get_start_key().is_min_rowkey()) { begin_idx = 0; }
    else if(0 > find_bound(range, 0, begin_idx, equal, end_key_begin_idx, end_key_end_idx)) { ret = -2; } // OB_BEYOND_THE_RANGE
    // else if(!range.get_border_flag().inclusive_start()) {
    else if(!((range.get_border_flag() & INCLUSIVE_START) == INCLUSIVE_START)) {
      if(equal) {
        ++begin_idx;
        if(begin_idx == row_count_) { ret = -3; } // OB_BEYOND_THE_RANGE
      }
    }
    // LOG_DEBUG
    if(0 == ret){
      if(!is_right_border || range.get_end_key().is_max_rowkey()) { end_idx = row_count_ - 1; }
      else if(end_key_begin_idx > end_key_end_idx) { ret = -4; } // OB_ERR_UNEXPECTED
      else {
        const bool is_precise_rowkey = read_info_->get_rowkey_count() == range.get_end_key().get_datum_cnt();
        // we should use upper_bound if the range include endkey
        if(0 > find_bound(range.get_end_key(),
                          // !range.get_border_flag().inclusive_end()/*lower_bound*/,
                          !((range.get_border_flag() & INCLUSIVE_END) == INCLUSIVE_END),
                          end_key_begin_idx > begin_idx ? end_key_begin_idx : begin_idx,
                          end_idx,
                          equal)) { ret = -5;
          //LOG_WARN("fail to get lower bound endkey", K(ret));
        }
        else if(end_idx == row_count_) { --end_idx; }
        // else if(is_index_block && !(equal && range.get_border_flag().inclusive_end() && is_precise_rowkey)) {} // Skip
        else if(is_index_block && !(equal && (range.get_border_flag() & INCLUSIVE_END) == INCLUSIVE_END && is_precise_rowkey)) {} // Skip
        //// When right border is closed and found rowkey is equal to end key of range, do --end_idx
        else if(end_idx == 0) { ret = -6; } //OB_BEYOND_THE_RANGE
        else { --end_idx; }
      }
    }
  }
  // LOG_DEBUG
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
