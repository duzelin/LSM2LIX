#include "log_table.h"

#include <cstdint>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "coding.h"
#include "crc32c.h"

namespace LSM2LIX {

namespace LOG {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = CRC32C::Value(&t, 1);
  }
}

LOG_Writer::LOG_Writer(int fd, std::string filename) : pos_(0), fd_(fd), block_offset_(0), filename_(filename) {
    InitTypeCrc(type_crc_);
}

LOG_Writer::LOG_Writer(int fd, uint64_t f_length, std::string filename) : pos_(0), fd_(fd), block_offset_(f_length % kBlockSize), filename_(filename) {
    InitTypeCrc(type_crc_);
}

LOG_Writer::~LOG_Writer() = default;

Status LOG_Writer::AddRecord(const Slice& slice) {
    const char* ptr = slice.data();
    size_t left = slice.size();

    Status s;
    bool begin = true;
    do {
        const int leftover = kBlockSize - block_offset_;
        assert(leftover >= 0);
        if (leftover < kHeaderSize) {
            if (leftover > 0) {
                static_assert(kHeaderSize == 7, "");
                AppendToFile(Slice("\x00\x00\x00\x00\x00\x00", leftover));
            }
            block_offset_ = 0;
        }
        assert(kBlockSize - block_offset_ -kHeaderSize >= 0);
        const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
        const size_t fragment_length = (left < avail) ? left : avail;
        RecordType type;
        const bool end = (left == fragment_length);
        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }
        s = EmitPhysicalRecord(type, ptr, fragment_length);
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (s.ok() && left > 0);
    return s;
}

Status LOG_Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t length) {
    assert(length <= 0xffff);
    assert(block_offset_ + kHeaderSize + length <= kBlockSize);
    // format the header
    char buf[kHeaderSize];
    buf[4] = static_cast<char>(length && 0xff);
    buf[5] = static_cast<char>(length >> 8);
    buf[6] = static_cast<char>(t);
    // compute the crc of the record type and the payload
    uint32_t crc = CRC32C::Extend(type_crc_[t], ptr, length);
    crc = CRC32C::Mask(crc);
    EncodeFixed32(buf, crc);
    // write the header and the payload
    Status s = AppendToFile(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = AppendToFile(Slice(ptr, length));
        if (s.ok()) {
            s = FlushBuffer();
        }
    }
    block_offset_ += kHeaderSize + length;
    return s;
}

Status LOG_Writer::AppendToFile(const Slice& data) {
    size_t write_size = data.size();
    const char* write_data = data.data();

    size_t copy_size = std::min(write_size, kLogBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
        return Status::OK();
    }
    Status status = FlushBuffer();
    if (!status.ok()) {
        return status;
    }
    if (write_size < kLogBufferSize) {
        std::memcpy(buf_, write_data, write_size);
        pos_ = write_size;
        return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
}

Status LOG_Writer::FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
}

Status LOG_Writer::WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
        ssize_t write_result = ::write(fd_, data, size);
        if (write_result < 0) {
            if (errno == EINTR) {
                continue; // Retry
            }
            return PosixError(filename_, errno);
        }
        data += write_result;
        size -= write_result;
    }
    return Status::OK();
}

LOG_Reader::Reporter::~Reporter() = default;

LOG_Reader::LOG_Reader(int fd, std::string filename, Reporter* reporter, bool checksum, uint64_t initial_offset) : 
    fd_(fd), 
    filename_(filename), 
    reporter_(reporter), 
    checksum_(checksum), 
    backing_store_(new char[kBlockSize]), 
    buffer_(), 
    eof_(false), 
    last_record_offset_(0), 
    end_of_buffer_offset_(0), 
    initial_offset_(initial_offset), 
    resyncing_(initial_offset > 0) {}

LOG_Reader::~LOG_Reader() { delete[] backing_store_; }

bool LOG_Reader::SkipToInitialBlock() {
    const size_t offset_in_block = initial_offset_ % kBlockSize;
    uint64_t block_start_location = initial_offset_ - offset_in_block;
    if (offset_in_block > kBlockSize - 6) {
        block_start_location += kBlockSize;
    }
    end_of_buffer_offset_ = block_start_location;
    if (block_start_location > 0) {
        Status skip_status = FileSkip(block_start_location);
        if (!skip_status.ok()) {
            ReportDrop(block_start_location, skip_status);
            return false;
        }
    }
    return true;
}

bool LOG_Reader::ReadRecord(Slice* record, std::string* scratch) {
    if (last_record_offset_ < initial_offset_) {
        if (!SkipToInitialBlock()) {
            return false;
        }
    }
    scratch->clear();
    record->clear();
    bool in_fragmented_record = false;
    uint64_t prospective_record_offset = 0;
    Slice fragment;
    while (true) {
        const unsigned int record_type = ReadPhysicalRecord(&fragment);
        uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();
        if (resyncing_) {
            if (record_type == kMiddleType) {
                continue;
            } else if (record_type == kLastType) {
                resyncing_ = false;
                continue;
            } else {
                resyncing_ = false;
            }
        }
        switch (record_type) {
            case kFullType:
                if (in_fragmented_record) {
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(), "partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->clear();
                *record = fragment;
                last_record_offset_ = prospective_record_offset;
                return true;
            case kFirstType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                }
                break;
            case kLastType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
                } else {
                    scratch->append(fragment.data(), fragment.size());
                    *record = Slice(*scratch);
                    last_record_offset_ = prospective_record_offset;
                    return true;
                }
                break;
            case kEof:
                if (in_fragmented_record) {
                    scratch->clear();
                }
                return false;
            case kBadRecord:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(), "error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                break;
            default: {
                char buf[40];
                std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
                ReportCorruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)), buf);
                in_fragmented_record = false;
                scratch->clear();
                break;
            }
        }
    }
    return false;
}

uint64_t LOG_Reader::LastRecordOffset() {return last_record_offset_;}

void LOG_Reader::ReportCorruption(uint64_t bytes, const char* reason) {
    ReportDrop(bytes, Status::Corruption(reason));
}

void LOG_Reader::ReportDrop(uint64_t bytes, const Status& reason) {
    if (reporter_ != nullptr && end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
        reporter_->Corruption(static_cast<size_t>(bytes), reason);
    }
}

unsigned int LOG_Reader::ReadPhysicalRecord(Slice* result) {
    while (true) {
        if (buffer_.size() < kHeaderSize) {
            if (!eof_) {
                // Last read was a full read, so this is a trailer to skip
                buffer_.clear();
                Status status = ReadFromFile(kBlockSize, &buffer_, backing_store_);
                end_of_buffer_offset_ += buffer_.size();
                if (!status.ok()) {
                buffer_.clear();
                ReportDrop(kBlockSize, status);
                eof_ = true;
                return kEof;
                } else if (buffer_.size() < kBlockSize) {
                eof_ = true;
                }
                continue;
            } else {
                // Note that if buffer_ is non-empty, we have a truncated header at the
                // end of the file, which can be caused by the writer crashing in the
                // middle of writing the header. Instead of considering this an error,
                // just report EOF.
                buffer_.clear();
                return kEof;
            }
        }
        // Parse the header
        const char* header = buffer_.data();
        const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
        const unsigned int type = header[6];
        const uint32_t length = a | (b << 8);
        if (kHeaderSize + length > buffer_.size()) {
        size_t drop_size = buffer_.size();
        buffer_.clear();
        if (!eof_) {
            ReportCorruption(drop_size, "bad record length");
            return kBadRecord;
        }
        // If the end of the file has been reached without reading |length| bytes
        // of payload, assume the writer died in the middle of writing the record.
        // Don't report a corruption.
        return kEof;
        }

        if (type == kZeroType && length == 0) {
        // Skip zero length record without reporting any drops since
        // such records are produced by the mmap based writing code in
        // env_posix.cc that preallocates file regions.
        buffer_.clear();
        return kBadRecord;
        }

        // Check crc
        if (checksum_) {
        uint32_t expected_crc = CRC32C::Unmask(DecodeFixed32(header));
        uint32_t actual_crc = CRC32C::Value(header + 6, 1 + length);
        if (actual_crc != expected_crc) {
            // Drop the rest of the buffer since "length" itself may have
            // been corrupted and if we trust it, we could find some
            // fragment of a real log record that just happens to look
            // like a valid log record.
            size_t drop_size = buffer_.size();
            buffer_.clear();
            ReportCorruption(drop_size, "checksum mismatch");
            return kBadRecord;
        }
        }

        buffer_.remove_prefix(kHeaderSize + length);

        // Skip physical record that started before initial_offset_
        if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
            initial_offset_) {
        result->clear();
        return kBadRecord;
        }

        *result = Slice(header + kHeaderSize, length);
        return type;
    }
}

Status LOG_Reader::FileSkip(uint64_t n) {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
        return PosixError(filename_, errno);
    }
    return Status::OK();
}

Status LOG_Reader::ReadFromFile(size_t n, Slice* result, char* scratch) {
    Status status;
    while (true) {
        ::ssize_t read_size = ::read(fd_, scratch, n);
        if (read_size < 0) {
            if (errno == EINTR) {
                continue;
            }
            status = PosixError(filename_, errno);
            break;
        }
        *result = Slice(scratch, read_size);
        break;
    }
    return status;
}

} // namespace LOG

} // namespace LSM2LIX