#ifndef KINGWOW_STORAGE_BLOCKSSTABLE_KW_MICRO_BLOCK_H_
#define KINGWOW_STORAGE_BLOCKSSTABLE_KW_MICRO_BLOCK_H_
#include <stdint.h>
// #include "kw_define.h"
// #include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{
// const int64_t MICRO_BLOCK_HEADER_MAGIC = 1005;
// const int64_t BF_MACRO_BLOCK_HEADER_MAGIC = 1014;
// const int64_t BF_MICRO_BLOCK_HEADER_MAGIC = 1015;
// const int64_t SERVER_SUPER_BLOCK_MAGIC = 1018;
// const int64_t LINKED_MACRO_BLOCK_HEADER_MAGIC = 1019;

// const int64_t MICRO_BLOCK_HEADER_VERSION_1 = 1;
// const int64_t MICRO_BLOCK_HEADER_VERSION_2 = 2;
// const int64_t MICRO_BLOCK_HEADER_VERSION = MICRO_BLOCK_HEADER_VERSION_2;
// const int64_t LINKED_MACRO_BLOCK_HEADER_VERSION = 1;
// const int64_t BF_MACRO_BLOCK_HEADER_VERSION = 1;
// const int64_t BF_MICRO_BLOCK_HEADER_VERSION = 1;

struct KwMicroBlockHeader
{
    static const int64_t COLUMN_CHECKSUM_PTR_OFFSET;
public:
    int16_t magic_;
    int16_t version_;
    uint32_t header_size_;
    int16_t header_checksum_;
    uint16_t column_count_;
    uint16_t rowkey_column_count_;
    struct {
        uint16_t has_column_checksum_ : 1;
        uint16_t has_string_out_row_ : 1; // flag for furture, varchar and char can be overflowed as lob handle
        uint16_t all_lob_in_row_ : 1; // compatible with 4.0, we assume that all lob is out row in old data
        uint16_t contains_hash_index_   : 1;
        uint16_t hash_index_offset_from_end_ : 10;
        uint16_t reserved16_          : 2;
    };
    uint32_t row_count_;
    uint8_t row_store_type_;
    union {
        struct {
        uint8_t row_index_byte_    :3;
        uint8_t extend_value_bit_  :3;
        uint8_t reserved_          :2;
        }; // For encoding format
        struct {
            uint8_t single_version_rows_: 1;
            uint8_t contain_uncommitted_rows_: 1;
            uint8_t is_last_row_last_flag_ : 1;
            uint8_t not_used_ : 5;
        }; // For flat format
        uint8_t opt_;
    };
    uint16_t var_column_count_; // For encoding format
    union {
        uint32_t row_index_offset_; // For flat format
        uint32_t row_data_offset_; // For encoding format
        uint32_t row_offset_;
    };
    int32_t original_length_;
    int64_t max_merged_trans_version_;
    int32_t data_length_;
    int32_t data_zlength_;
    int64_t data_checksum_;
    int64_t *column_checksums_;
public:
    KwMicroBlockHeader();
    ~KwMicroBlockHeader() = default;
    void reset() { new (this) KwMicroBlockHeader(); }
    bool check_data_valid() const;
    bool is_valid() const;
    void set_header_checksum();
    int check_header_checksum() const;
    int check_payload_checksum(const char *buf, const int64_t len) const;
    static int deserialize_and_check_record(
        const char *ptr, const int64_t size,
        const int16_t magic, const char *&payload_ptr, int64_t &payload_size);
    static int deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic);
    int check_and_get_record(
        const char *ptr, const int64_t size, const int16_t magic,
        const char *&payload_ptr, int64_t &payload_size) const;
    int check_record(const char *ptr, const int64_t size, const int16_t magic) const;
    int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
    int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
    uint32_t get_serialize_size() { return get_serialize_size(column_count_, has_column_checksum_); }
    static uint32_t get_serialize_size(const int64_t column_count, const bool need_calc_column_chksum) {
        return static_cast<uint32_t>(KwMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET  +
            (need_calc_column_chksum ? column_count * sizeof(int64_t) : 0));
    }
    TO_STRING_KV(K_(magic), K_(version), K_(header_size), K_(header_checksum),
        K_(column_count), K_(rowkey_column_count), K_(has_column_checksum), K_(row_count), K_(row_store_type),
        K_(opt), K_(var_column_count), K_(row_offset), K_(original_length), K_(max_merged_trans_version),
        K_(data_length), K_(data_zlength), K_(data_checksum), KP_(column_checksums), K_(single_version_rows),
        K_(contain_uncommitted_rows),  K_(is_last_row_last_flag), K(is_valid()));
    int test_test(){ return 7371; }
public:
    bool is_compressed_data() const { return data_length_ != data_zlength_; }
    bool contain_uncommitted_rows() const { return contain_uncommitted_rows_; }
    inline bool has_string_out_row() const { return has_string_out_row_; }
    inline bool has_lob_out_row() const { return !all_lob_in_row_; }
    bool is_last_row_last_flag() const { return is_last_row_last_flag_; }
    bool is_contain_hash_index() const;
}__attribute__((packed));  
} // end namespace blocksstable
} // end namespace kingwow
#endif