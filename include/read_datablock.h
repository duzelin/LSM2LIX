#ifndef READ_DATABLOCK_H
#define READ_DATABLOCK_H

#include <cstddef>
#include <cstdint>

#include "slice.h"
#include "status.h"

namespace LSM2LIX {

// class BlockHandle {
//     public:
//     // Maximum encoding length of a BlockHandle
//     enum { kMaxEncodedLength = 10 + 10 };

//     BlockHandle();

//     // The offset of the block in the file.
//     uint64_t offset() const { return offset_; }
//     void set_offset(uint64_t offset) { offset_ = offset; }

//     // The size of the stored block
//     uint64_t size() const { return size_; }
//     void set_size(uint64_t size) { size_ = size; }

//     void EncodeTo(std::string* dst) const;
//     Status DecodeFrom(Slice* input);

//     private:
//     uint64_t offset_;
//     uint64_t size_;
// };
struct BlockHandle {
    uint64_t offset_;
    uint64_t size_;
};

struct BlockContents {
    Slice data;
};
class Comparator;

class Iterator {
    public:
    Iterator() {};

    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    virtual ~Iterator() {};

    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    virtual bool Valid() const = 0;

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last key in the source.  The iterator is
    // Valid() after this call iff the source is not empty.
    virtual void SeekToLast() = 0;

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    virtual void Seek(const Slice& target) = 0;

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Return the key for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    virtual Slice key() const = 0;

    // Return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    virtual Slice value() const = 0;

    // If an error has occurred, return it.  Else return an ok status.
    virtual Status status() const = 0;
};

class Block {
    public:
    explicit Block(const char* data, size_t size);
    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;
    ~Block() {};
    
    Iterator* NewIterator(const Comparator* comparator);

    private:
    class Iter;
    
    uint32_t NumRestarts() const;

    const char* data_;
    size_t size_;
    uint32_t restart_offset_;
};

// class Footer {
//  public:
//   // Encoded length of a Footer.  Note that the serialization of a
//   // Footer will always occupy exactly this many bytes.  It consists
//   // of two block handles and a magic number.
//   enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

//   Footer() = default;

//   // The block handle for the metaindex block of the table
//   const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
//   void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

//   // The block handle for the index block of the table
//   const BlockHandle& index_handle() const { return index_handle_; }
//   void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

//   void EncodeTo(std::string* dst) const;
//   Status DecodeFrom(Slice* input);

//  private:
//   BlockHandle metaindex_handle_;
//   BlockHandle index_handle_;
// };

} // namespace Reader

#endif