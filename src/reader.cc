#include "reader.h"

#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <stdlib.h>
#include <unistd.h>

#include "status.h"
#include "comparator.h"

namespace LSM2LIX {

static const size_t kBlockTrailerSize = 5;

// Status PosixError(const std::string& context, int error_number) {
//     if (error_number == ENOENT) {
//         return Status::NotFound(context, std::strerror(error_number));
//     } else {
//         return Status::IOError(context, std::strerror(error_number));
//     }
// }

Reader::Reader() {
    comparator_ = BytewiseComparator();
}

Reader::~Reader() {
    delete comparator_;
    // To do check the free() invalid pointer.
    if (block_) {
        delete block_;
    }
    if (iter_) {
        delete iter_;
    }  
}

void Reader::AllocateBuf()
{
    buf = aligned_alloc(4096UL, 4096*2);
}

void Reader::FreeBuf()
{
    free(buf);
}

void Reader::SetSSTFileName(std::string& filename) {
    filename_ = filename;
    fd_ = 0;
}

Status Reader::ReadBlockContents(BlockHandle& handle) {
    Status status;
    int fd = fd_;
    // fd = ::open(filename_.c_str(), O_RDONLY | O_DIRECT);
    fd = ::open(filename_.c_str(), O_RDONLY);
    if (fd < 0) {
        return PosixError(filename_, errno);
    }
    size_t length = static_cast<size_t>(handle.size_);
    ssize_t read_size = ::pread(fd, buf, length, static_cast<off_t>(handle.offset_));
    if (read_size < 0) {
        status = PosixError(filename_, errno);
    }
    ::close(fd);
    if (status.ok()) {
        block_ = new Block((char*)buf, read_size);
        iter_ = block_->NewIterator(comparator_);
    }
    return status;
}

void Reader::ReleaseBlockContents() {
    delete block_;
    delete iter_;
    block_ = nullptr;
    iter_ = nullptr;
}

Status Reader::Get(const Slice& key, std::string* value) {
    Status status;
    iter_->Seek(key);
    if (iter_->Valid()) {
        status = Status::OK();
        value->assign(iter_->value().data(), iter_->value().size());
    }
    else {
        status = Status::NotFound("Can not find the value corresponding to the target key.");
    }
    return status;
}

} // namespace Reader