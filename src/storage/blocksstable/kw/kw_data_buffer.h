#ifndef __KW_BLOCK_SSTABLE_DATA_BUFFER_
#define __KW_BLOCK_SSTABLE_DATA_BUFFER_
#include <stdint.h>
#include <stdarg.h>
#include <algorithm>
#include "lib/allocator/ob_fixed_size_block_allocator.h"
#include "lib/utility/utility.h"
// #include "share/ob_define.h"
// #include "common/log/ob_log_constants.h"

namespace oceanbase
{
namespace blocksstable
{
class KwBufferHolder
{
public:
    inline char *data(){ return data_; }
    inline const char *data() const { return data_; }
    inline bool is_valid() const { return (NULL != data_) && (capacity_ > 0); }
    inline char *current() { return data_ + pos_; }
    inline const char *current() const { return data_ + pos_; }
    inline int64_t pos() const { return pos_; }
    inline int64_t length() const { return pos_; }
    inline int64_t remain() const { return capacity_ - pos_; }
    inline int64_t capacity() const { return capacity_; }
    inline int advance(const int64_t length)
    {
        if(remain() < length){ return -1; }
        pos_ += length;
        return 0;
    }
    inline int advance_zero(const int64_t length)
    {
        if(remain() < length){ return -1; }
        memset(current(), 0, length);
        pos_ += length;
        return 0;
    }
    inline int backward(const int64_t length)
    {
        if(pos_ < length){ return -1; }
        pos_ -= length;
        return 0;
    }
    inline int set_pos(const int64_t pos){
        if(pos < 0 || pos > capacity_){ return -1; }
        pos_ = pos;
        return 0;
    }
    //to string
    TO_STRING_KV(KP_(data), K_(pos), K_(capacity));
public:
    KwBufferHolder() : data_(NULL), pos_(0), capacity_(0) {}
    virtual ~KwBufferHolder() {}
    // INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
    inline int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
        int ret = 0;
        const int64_t serialize_size = get_serialize_size();
        //Null ObString is allowed
        if (NULL == buf || serialize_size > buf_len - pos) {
            ret = -1;//common::OB_SIZE_OVERFLOW;
            // LIB_LOG(WARN, "size overflow", K(ret),
            //     KP(buf), K(serialize_size), "remain", buf_len - pos);
        } else if (0 > (ret = common::serialization::encode_vstr(buf, buf_len, pos, data_, pos_))) {
            // LIB_LOG(WARN, "string serialize failed", K(ret));
        }
        return ret;
    }
    inline int deserialize(char *buf, const int64_t data_len, int64_t &pos) //const
    {
        int ret = 0;
        int64_t len = 0;
        const int64_t MINIMAL_NEEDED_SIZE = 2; // at least need 2 bytes
        if(NULL == buf || data_len - pos < MINIMAL_NEEDED_SIZE) {
            ret = -1;
            // LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
        }
        else {
            data_ = const_cast<char *>(common::serialization::decode_vstr(buf, data_len, pos, &len));
            if(NULL == data_) { 
                ret = -2;
                // LIB_LOG(WARN, "decode NULL string", K(ret));
            }
            else {
                capacity_ = len;
                pos_ = 0;
            }
        }
        return ret;
    }
    inline int64_t get_serialize_size() const
    { return common::serialization::encoded_length_vstr(pos_); }
protected:
    char *data_;
    int64_t pos_;
    int64_t capacity_;
};

template <bool> struct KwBufferWriteWrap;
template <>
struct KwBufferWriteWrap<true>
{
  template <typename T, typename U>
  int operator()(T &t, const U &u) { return t.write_serialize(u); }
};
template <>
struct KwBufferWriteWrap<false>
{
  template <typename T, typename U>
  int operator()(T &t, const U &u) { return t.write_pod(u); }
};

template <bool> struct KwBufferReadWrap;
template <>
struct KwBufferReadWrap<true>
{
  template <typename T, typename U>
  int operator()(T &t, U &u) { return t.read_serialize(u); }
};
template <>
struct KwBufferReadWrap<false>
{
  template <typename T, typename U>
  int operator()(T &t, U &u) { return t.read_pod(u); }
};


class KwBufferWriter : public KwBufferHolder
{
public:
    template <typename T>
    int rollback(const T &value)
    { UNUSED(value); return backward(sizeof(T)); }

    template <typename T>
    int write_pod(const T &value) 
    {
        int ret = 0;
        if(remain() >= (int64_t)(sizeof(T))) { do_write(value); }
        else if(0 == (ret = expand(static_cast<int64_t>(sizeof(T))))) {
            // ASSERT(remain() > sizeof(T))
            do_write(value);
        }
        return ret;
    }

    int write(const char *buf, const int64_t size)
    {
        int ret = 0;
        if (remain() >= size) {
            do_write(buf, size);
        } else if (0 == (ret = expand(size))) {
            // ASSERT(remain() > sizeof(T))
            do_write(buf, size);
        }
        return ret;
    }

    template <typename T>
    int write_serialize(const T &value)
    {
        int ret = 0;
        int64_t serialize_size = value.get_serialize_size();
            if (remain() >= serialize_size) {
            ret = value.serialize(data_, capacity_, pos_);
        } else if (0 == (ret = expand(serialize_size))) {
            // ASSERT(remain() > sizeof(T))
            ret = value.serialize(data_, capacity_, pos_);
        }
        return ret;
    }
    
    template <typename T>
    int write(const T &value)
    {
        typedef KwBufferWriteWrap<HAS_MEMBER(T, get_serialize_size)> WRAP;
        return WRAP()(*this, value);
    }

    int append_fmt(const char *fmt, ...) __attribute__((format(printf, 2, 3)))
    {
        int rc = 0;
        va_list ap;
        va_start(ap, fmt);
        rc = vappend_fmt(fmt, ap);
        va_end(ap);
        return rc;
    }

    int vappend_fmt(const char *fmt, va_list ap)
    {
        int rc = 0;
        va_list ap2;
        va_copy(ap2, ap);
        int64_t n = vsnprintf(data_ + pos_, remain(), fmt, ap);
        if (n++ < 0) { // include '\0' at tail
            // _OB_LOG_RET(WARN, common::OB_ERR_SYS, "vsnprintf failed, errno %d", errno);
            rc = -1;//common::OB_ERROR;
        } 
        else if (n > remain()) {
            rc = expand(n + 1);
            if (0 == rc) {
                n = vsnprintf(data_ + pos_, remain(), fmt, ap2);
                if (n < 0) {
                    // _OB_LOG_RET(WARN, common::OB_ERR_SYS, "vsnprintf failed, errno %d", errno);
                    rc = -2;//common::OB_ERROR;
                } 
                else { pos_ += n; }
            }
        } 
        else { pos_ += n; }
        va_end(ap2);
        return rc;
    }

    virtual int expand(const int64_t size)
    {
        UNUSED(size);
        return -111;//common::OB_BUF_NOT_ENOUGH;
    }
    
    // bool is_dirty() const { return NULL != data_ && pos_ > 0; }
    // int ensure_space(int64_t size);

public:
    virtual ~KwBufferWriter() {}
    KwBufferWriter(char *buf, const int64_t data_len, int64_t pos = 0)
    { assign(buf, data_len, pos); }
    void assign(char *buf, const int64_t data_len, int64_t pos = 0)
    {
        data_ = const_cast<char *>(buf);
        capacity_ = data_len;
        pos_ = pos;
    }
protected:
    // write with no check;
    // caller ensure memory not out of bounds.
    template <typename T>
    void do_write(const T &value)
    {
        *((T *)(data_ + pos_)) = value;
        pos_ += sizeof(T);
    }

    void do_write(const char *buf, const int64_t size)
    {
        memcpy(current(), buf, size);
        pos_ += size;
    }
};

/*
 * write data buffer, auto expand memory when write overflow.
 * allocate memory for own self;
 */
class KwSelfBufferWriter : public KwBufferWriter
{
static const int64_t BUFFER_ALIGN_SIZE = 4096L;//common::ObLogConstants::LOG_FILE_ALIGN_SIZE;
public:
    virtual int expand(const int64_t size)
    {
        int64_t new_size = std::max(capacity_ * 2, static_cast<int64_t>(size + capacity_));
        if(is_aligned_) { new_size = common::upper_align(new_size, BUFFER_ALIGN_SIZE); }
        return ensure_space(new_size);
    }
public:
    KwSelfBufferWriter(const char *label, const int64_t size = 0, const bool need_align = false);
    virtual ~KwSelfBufferWriter();
    int ensure_space(const int64_t size);
    bool is_dirty() const { return NULL != data_ && pos_ > 0; }
    inline void reuse() { pos_ = 0; }
    inline void reset() { free(); pos_ = 0; capacity_ = 0; }
    inline void set_prop(const char *label, const bool need_align)
    { label_ = label; is_aligned_ = need_align; }
private:
    char *alloc(const int64_t size);
    void free();
private:
    const char *label_;
    bool is_aligned_;
    common::ObMacroBlockSizeMemoryContext macro_block_mem_ctx_;
};

class KwBufferReader : public KwBufferHolder
{
public:
    template <typename T>
    int read_pod(T &value)
    {
        int ret = 0;//common::OB_SUCCESS;

        if (remain() >= static_cast<int64_t>(sizeof(T))) {
            value = *(reinterpret_cast<T *>(current())) ;
            pos_ += (static_cast<int64_t>(sizeof(T)));
        } 
        else { ret = -1;/*common::OB_BUF_NOT_ENOUGH;*/ }
        return ret;
    }

    template <typename T>
    int read_serialize(T &value)
    { return value.deserialize(data_, capacity_, pos_); }

    template <typename Allocator, typename T>
    int read(Allocator &allocate, T &value)
    { return value.deserialize(allocate, data_, capacity_, pos_); }

    int read(char *buf, const int64_t size)
    {
        int ret = 0;//common::OB_SUCCESS;
        if (remain() >= size) {
            memcpy(buf, current(), size);
            pos_ += size;
        } 
        else { ret = -1;/*common::OB_BUF_NOT_ENOUGH;*/ }
        return ret;
    }

    int read_cstr(char *&buf)
    {
        int ret = 0;//common::OB_SUCCESS;
        buf = current();
        while (pos_ < capacity_ && 0 != *(data_ + pos_)) { ++pos_; }
        if (pos_ == capacity_) { ret = -1;/*common::OB_BUF_NOT_ENOUGH;*/ } 
        else { ++pos_; /* skip trailing '\0'*/ }
        return ret;
    }

    template <typename T>
    int read(T &value)
    {
        typedef KwBufferReadWrap<HAS_MEMBER(T, get_serialize_size)> WRAP;
        return WRAP()(*this, value);
    }

    template <typename T>
    int get(const T *&value)
    {
        int ret = 0;//common::OB_SUCCESS;
        if (OB_UNLIKELY((capacity_ - pos_) < static_cast<int64_t>(sizeof(T)))) {
            ret = -1;//common::OB_BUF_NOT_ENOUGH;
        } 
        else {
            value = (const T *)(data_ + pos_) ;
            pos_ += (static_cast<int64_t>(sizeof(T)));
        }
        return ret;
    }
public:
    KwBufferReader() 
    { assign(NULL, 0, 0); }
    KwBufferReader(const char *buf, const int64_t data_len, int64_t pos = 0)
    { assign(buf, data_len, pos); }
    ~KwBufferReader() {}

    KwBufferReader(const KwBufferReader &other)
    {
        if(this != &other) {
            data_ = other.data_;
            capacity_ = other.capacity_;
            pos_ = other.pos_;
        }
    }

    KwBufferReader &operator = (const KwBufferReader &other)
    {
        if(this != &other) {
            data_ = other.data_;
            capacity_ = other.capacity_;
            pos_ = other.pos_;
        }
        return *this;
    }

    void assign(const char *buf, const int64_t data_len, int64_t pos = 0)
    {
        data_ = const_cast<char *>(buf);
        capacity_ = data_len;
        pos_ = pos;
    }
};

} // namespace blocksstable
} // namespace oceanbase

#endif