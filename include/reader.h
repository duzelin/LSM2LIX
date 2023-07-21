#ifndef READER_H
#define READER_H

#include "status.h"
#include "read_datablock.h"

namespace LSM2LIX {

class Reader {
    public:
    explicit Reader();
    ~Reader();
    void AllocateBuf();
    void FreeBuf();
    void SetSSTFileName(std::string& filename);
    Status ReadBlockContents(BlockHandle& handle);
    void ReleaseBlockContents();
    Status Get(const Slice& key, std::string* value);

    private:
    std::string filename_;
    int fd_;
    void* buf;
    Block* block_ = nullptr;
    Block* index_block_ = nullptr;
    const Comparator* comparator_ = nullptr;
    Iterator* iter_= nullptr;

};

} // namespace Reader

#endif