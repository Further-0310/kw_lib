#include "kw_data_buffer.h"
#include <malloc.h>
#include "lib/allocator/ob_malloc.h"
// #include "share/rc/ob_tenant_base.h"
using namespace oceanbase;
using namespace common;

namespace oceanbase
{
namespace blocksstable
{

KwSelfBufferWriter::KwSelfBufferWriter(const char *label, const int64_t size, const bool need_align)
    : KwBufferWriter(NULL, 0, 0), label_(label), is_aligned_(need_align),
      macro_block_mem_ctx_()
{
    int ret = 0;//OB_SUCCESS;
    if (0 > (ret = macro_block_mem_ctx_.init())) {
        // STORAGE_LOG(WARN, "fail to init macro block memory context.", K(ret));
    } else if (0 > (ret = ensure_space(size))) {
        // STORAGE_LOG(WARN, "cannot allocate memory for data buffer.", K(size), K(ret));
    }
}

KwSelfBufferWriter::~KwSelfBufferWriter()
{
    free();
    is_aligned_ = false;
    pos_ = 0;
    capacity_ = 0;
    macro_block_mem_ctx_.destroy();
}

char *KwSelfBufferWriter::alloc(const int64_t size)
{
    char *data = NULL;
#ifndef OB_USE_ASAN
    if (size == macro_block_mem_ctx_.get_block_size()) {
        data = (char *)macro_block_mem_ctx_.alloc();
        if (NULL == data) {}// warn "fail to alloc buf from mem ctx"
        // STORAGE_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to alloc buf from mem ctx", K(size));
    }
#endif
    if(NULL == data){
        if (is_aligned_) { 
            // data = (char *)mtl_malloc_align(BUFFER_ALIGN_SIZE, size, label_);
            data = (char *)ob_malloc_align(BUFFER_ALIGN_SIZE, size, label_);
        } 
        else { 
            // data = (char *)mtl_malloc(size, label_); 
            data = (char *)ob_malloc(size, label_); 
        }
    }
    return data;
}

int KwSelfBufferWriter::ensure_space(int64_t size)
{
    int ret = 0;
    //size = upper_align(size, common::ObLogConstants::LOG_FILE_ALIGN_SIZE);
    if (size <= 0){} // do nothing.
    else if (is_aligned_ && size % (4 << 10) != 0){ ret = -1; } // warn "not aligned buffer size" 
    //static const int64_t BUFFER_ALIGN_SIZE = common::ObLogConstants::LOG_FILE_ALIGN_SIZE = 4 << 10; // 4KB
    else if(NULL == data_){
        if(NULL == (data_ = alloc(size))){ ret = -2; }// warn "allocate buffer memory error."
        else { pos_ = 0; capacity_ = size; }
    }
    else if(capacity_ < size){//resize
        char *new_data = NULL;
        if(NULL == (new_data = alloc(size))){ ret = -3; }// warn "allocate buffer memory error."
        else {
            memcpy(new_data, data_, pos_);
            free();
            capacity_ = size;
            data_ = new_data;
        }
    }
    return ret;
}

void KwSelfBufferWriter::free()
{
    if (NULL == data_){
#ifndef OB_USE_ASAN
        if (macro_block_mem_ctx_.get_allocator().contains(data_)) {
            macro_block_mem_ctx_.free(data_);
            data_ = NULL;
        }
#endif
        if(NULL != data_) {
            // if (is_aligned_) { mtl_free_align(data_); } 
            if (is_aligned_) { ob_free_align(data_); } 
            // else { mtl_free(data_); }
            else { ob_free(data_); }
            data_ = NULL;
        }
    }
}
}
}