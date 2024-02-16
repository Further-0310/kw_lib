#define USING_LOG_PREFIX STORAGE
#include "kw_micro_block_header.h"
#include "kw_block_sstable_struct.h"
// #include "common/ob_store_format.h"

namespace oceanbase
{
namespace blocksstable
{
/*********************ob_record_header.h************************/
inline void format_i64(const int64_t value, int16_t &check_sum)
{
  int i = 0;
  while (i < 4) {
    check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
    ++i;
  }
}

inline void format_i32(const int32_t value, int16_t &check_sum)
{
  int i = 0;
  while (i < 2) {
    check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
    ++i;
  }
}

const int64_t KwMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET = 64;
KwMicroBlockHeader::KwMicroBlockHeader()
    :   magic_(/*MICRO_BLOCK_HEADER_MAGIC*/1005),
        version_(/*MICRO_BLOCK_HEADER_VERSION*/2),
        header_size_(0),
        header_checksum_(0),
        column_count_(0),
        has_column_checksum_(0),
        reserved16_(0),
        row_count_(0),
        row_store_type_(MAX_ROW_STORE),
        opt_(0),
        var_column_count_(0),
        row_offset_(0),
        original_length_(0),
        max_merged_trans_version_(0),
        data_length_(0),
        data_zlength_(0),
        data_checksum_(0),
        column_checksums_(nullptr)
    {
        // COLUMN_CHECKSUM_PTR_OFFSET is the length of fixed component in micro header,
        // column checksum will be stored from this offset. If assert failed, please check.
        // STATIC_ASSERT(COLUMN_CHECKSUM_PTR_OFFSET == sizeof(KwMicroBlockHeader), "checksum offset mismatch");
        static_assert(COLUMN_CHECKSUM_PTR_OFFSET == sizeof(KwMicroBlockHeader), "checksum offset mismatch");
    }

bool KwMicroBlockHeader::is_valid() const
{//参数判断微块header是否合法
    bool valid_data = 
        header_size_ == get_serialize_size(column_count_, has_column_checksum_)
        && version_ >= /*MICRO_BLOCK_HEADER_VERSION_1*/1
        && /*MICRO_BLOCK_HEADER_MAGIC*/1005 == magic_
        && column_count_ >= rowkey_column_count_
        && rowkey_column_count_ > 0
        && var_column_count_ <= column_count_
        && row_store_type_ < MAX_ROW_STORE; // KwMicroBlockHeader::is_valid
    return valid_data;
}

void KwMicroBlockHeader::set_header_checksum()
{//设置header的校验和：应该是把各个属性都用上
    int16_t checksum = 0;
    header_checksum_ = 0;

    checksum = checksum ^ magic_;
    checksum = checksum ^ version_;
    checksum = checksum ^ static_cast<int16_t>(row_store_type_);
    checksum = checksum ^ static_cast<int16_t>(opt_);

    format_i32(column_count_, checksum);
    format_i32(rowkey_column_count_, checksum);
    format_i32(has_column_checksum_, checksum);
    format_i32(var_column_count_, checksum);

    format_i64(header_size_, checksum);
    format_i64(row_count_, checksum);
    format_i64(row_offset_, checksum);
    format_i64(original_length_, checksum);
    format_i64(max_merged_trans_version_, checksum);
    format_i64(data_length_, checksum);
    format_i64(data_zlength_, checksum);
    format_i64(data_checksum_, checksum);

    if (has_column_checksum_) {
        for (int64_t i = 0; i < column_count_; ++i) {
            format_i64(column_checksums_[i], checksum);
        }
    }
    header_checksum_ = checksum;
}

int KwMicroBlockHeader::check_header_checksum() const
{//检查header校验和：最后结果不为0即物理校验和出错
    int ret = 0;//OB_SUCCESS;
    int16_t checksum = 0;

    checksum = checksum ^ magic_;
    checksum = checksum ^ version_;
    checksum = checksum ^ header_checksum_;
    checksum = checksum ^ static_cast<int16_t>(row_store_type_);
    checksum = checksum ^ static_cast<int16_t>(opt_);

    format_i32(column_count_, checksum);
    format_i32(rowkey_column_count_, checksum);
    format_i32(has_column_checksum_, checksum);
    format_i32(var_column_count_, checksum);

    format_i64(header_size_, checksum);
    format_i64(row_count_, checksum);
    format_i64(row_offset_, checksum);
    format_i64(original_length_, checksum);
    format_i64(max_merged_trans_version_, checksum);
    format_i64(data_length_, checksum);
    format_i64(data_zlength_, checksum);
    format_i64(data_checksum_, checksum);
    if (has_column_checksum_) {
        for (int64_t i = 0; i < column_count_; ++i) {
            format_i64(column_checksums_[i], checksum);
        }
    }

    if(0 != checksum) {
        // 物理校验和出错，DBA error log
        ret = -1;//OB_PHYSIC_CHECKSUM_ERROR;
        // LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg","record check checksum failed", K(ret), K(*this));
    }
    return ret;
}

int KwMicroBlockHeader::check_payload_checksum(const char *buf, const int64_t len) const
{//检查payload校验和：方法是crc64_sse42
  int ret = 0;//OB_SUCCESS;
  if (NULL == buf || len < 0 || data_zlength_ != len
      || (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_))) {
    // 非法参数，warn
    ret = -1;//OB_INVALID_ARGUMENT;
  } else {
    // 用crc64_sse42校验*********************************
    const int64_t data_checksum = ob_crc64_sse42(buf, len);
    if (data_checksum != data_checksum_) {
        // 物理校验和出错，DBA error log
        ret = -2;//OB_PHYSIC_CHECKSUM_ERROR;
        // LOG_DBA_ERROR
    }
  }
  return ret;
}

int KwMicroBlockHeader::deserialize_and_check_record(
    const char *ptr, const int64_t size,
    const int16_t magic, const char *&payload_ptr, int64_t &payload_size)
{//header的反序列化并检查记录
    int ret = 0;//OB_SUCCESS;
    KwMicroBlockHeader header;
    int64_t pos = 0;
    if (NULL == ptr || size < 0 || magic < 0) {
        // 非法参数，warn log
        ret = -1;//OB_INVALID_ARGUMENT;
    } else if(0 > header.deserialize(ptr, size, pos)){ ret = -2;
        // 若失败需要log warn "fail to deserialize header"
    } else if(0 > header.check_and_get_record(ptr, size, magic, payload_ptr, payload_size)){ ret = -3;
        // 若失败需要log warn "fail to check and get record"
    }
    return ret;
}

int KwMicroBlockHeader::check_and_get_record(
    const char *ptr, const int64_t size, const int16_t magic,
    const char *&payload_ptr, int64_t &payload_size) const
{//检查header校验和，检查缓冲区大小，检查payload的校验和
    int ret = 0;//OB_SUCCESS;
    if (nullptr == ptr || magic < 0){ ret = -1;
        // OB_INVALID_ARGUMENT, warn
    } else if(magic != magic_){ ret = -2;
        // OB_INVALID_DATA, warn
    } else if(0 > check_header_checksum()){ ret = -3;
        // 若失败需要log warn "fail to check header checksum"
    } else {
        const int64_t header_size = header_size_;
        if(size < header_size){ ret = -4;
            // OB_BUF_NOT_ENOUGH, warn
        } else{
            payload_ptr = ptr;
            payload_size = size;
            if(0 > check_payload_checksum(ptr + header_size, size - header_size)){ ret = -5;
                // 若失败需要log warn "fail to check payload checksum"
            }
        }
    }
    return ret;
}

int KwMicroBlockHeader::check_record(const char *ptr, const int64_t size, const int16_t magic) const
{//检查记录
    const char *payload_ptr = nullptr;
    int64_t payload_size = 0;
    if (nullptr == ptr || size < 0 || magic < 0) {
        // 非法参数，log warn
    } else{
        check_and_get_record(ptr, size, magic, payload_ptr, payload_size);
        // 若失败需要log warn "fail to check record"
    }
    return 0;
}

int KwMicroBlockHeader::deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic)
{//反序列化微块header并检查记录
  int ret = 0;//OB_SUCCESS;
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || magic < 0) { ret = -1;
    // OB_INVALID_ARGUMENT, warn
  } else if(0 > deserialize_and_check_record(ptr, size, magic, payload_buf, payload_size)){ ret = -2;
    // 若失败需要log warn "fail to check record"
  }
  return ret;
}

int KwMicroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{//序列化：header指针映射到buf开端，header的列校验和指针映射到buf+列检验和指针偏移量，之后复制列检验和数组到header的列校验和指针开始处，也就是写入列校验和
    int ret = 0;//OB_SUCCESS;
    if (nullptr == buf
        || buf_len < pos + COLUMN_CHECKSUM_PTR_OFFSET
        || buf_len < pos + header_size_
        || !is_valid()){//UNLIKELY，大概率执行else
            ret = -1;// OB_INVALID_ARGUMENT, warn
    } else {
        buf += pos;
        int64_t new_pos = 0;

        const int64_t serialize_size = header_size_;
        KwMicroBlockHeader *header = reinterpret_cast<KwMicroBlockHeader *>(buf);
        *header = *this;
        new_pos += COLUMN_CHECKSUM_PTR_OFFSET;
        if (has_column_checksum_) {
            header->column_checksums_ = reinterpret_cast<int64_t *>(buf + new_pos);
            if (NULL == column_checksums_) {//ISNULL，大概率执行else
                ret = -2;// OB_ERR_UNEXPECTED, warn
            } else {
                memcpy(header->column_checksums_, column_checksums_, column_count_ * sizeof(int64_t));
                new_pos += column_count_ * sizeof(int64_t);
                header->column_checksums_ = nullptr; // 总是序列化为空指针
            }
        } else {
            header->column_checksums_ = nullptr;
        }
        pos += new_pos;
    }
    return ret;
}

int KwMicroBlockHeader::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{//反序列化：
    int ret = 0;//OB_SUCCESS;
    if (nullptr == buf || buf_len < pos + COLUMN_CHECKSUM_PTR_OFFSET) {
        ret = -1;// OB_INVALID_ARGUMENT, warn
    } else {
        buf += pos;
        int64_t new_pos = 0;

        const KwMicroBlockHeader *header = reinterpret_cast<const KwMicroBlockHeader *> (buf);
        if(nullptr == header
            || !header->is_valid()
            || buf_len < pos + header->header_size_){//UNLIKELY，大概率执行else
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid header to deserialize", K(ret), K(buf_len), K(pos), KPC(header));
        } else {
            *this = *header;
            new_pos += COLUMN_CHECKSUM_PTR_OFFSET;
            if (has_column_checksum_) {
                const int64_t * column_checksums = nullptr;
                column_checksums = reinterpret_cast<const int64_t *> (buf + new_pos);
                column_checksums_ = const_cast<int64_t *> (column_checksums);
                new_pos += column_count_ * sizeof(int64_t);
            } else {
                column_checksums_ = nullptr;
            }
            pos += new_pos;
        }
    }
    return ret;
}

bool KwMicroBlockHeader::is_contain_hash_index() const
{
    return version_ >= /*MICRO_BLOCK_HEADER_VERSION_2*/2 && contains_hash_index_ == 1;
}

} // end namespace blocksstable    
} // end namespace kingwow
