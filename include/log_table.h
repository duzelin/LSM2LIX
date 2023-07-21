#ifndef LOG_TABLE_H
#define LOG_TABLE_H

#include <cstdint>
#include <dirent.h>

#include "slice.h"
#include "status.h"

#define tLOG_suffix "tLOG"
#define mLOG_suffix "mLOG"
#define tLOG_type 0
#define mLOG_type 1


namespace LSM2LIX {

namespace LOG {

static int tLOGFilter(const dirent* f_ptr) {
    const char* file_suffix = strrchr(f_ptr->d_name, '.');
    if (file_suffix != nullptr && strcmp(file_suffix, tLOG_suffix) == 0) {
        return 1;
    }
    return 0;
}

static int mLOGFilter(const dirent* f_ptr) {
    const char* file_suffix = strrchr(f_ptr->d_name, '.');
    if (file_suffix != nullptr && strcmp(file_suffix, mLOG_suffix) == 0) {
        return 1;
    }
    return 0;
}

static Status LoadFileList(const std::string path, std::vector<std::string>* file_list, int log_type) {
    dirent** f_list;
    int (*log_filter)(const dirent* f_ptr);
    if (log_type == tLOG_type) {
        log_filter = &tLOGFilter;
    } else {
        log_filter = &mLOGFilter;
    }
    int count = scandir(path.c_str(), &f_list, log_filter, versionsort);
    if (count < 0) {
        return Status::IOError("Scan dir failed.");
    }
    file_list->clear();
    for (int i = 0; i < count; i++) {
        file_list->push_back(f_list[i]->d_name); // d_name does not include the path!!
        free(f_list[i]);
    }
    free(f_list);
    return Status::OK();
}

enum RecordType {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};

static const int kMaxRecordType = kLastType;

static const int kBlockSize = 4 * 1024; // 4KB

static const int kHeaderSize = 4 + 2 + 1;

static const int kLogBufferSize = 1024;

class LOG_Writer {
    public:
    explicit LOG_Writer(int fd, std::string filename);
    LOG_Writer(int fd, uint64_t f_length, std::string filename);

    LOG_Writer(const LOG_Writer&) = delete;
    LOG_Writer& operator = (const LOG_Writer&) = delete;

    ~LOG_Writer();

    Status AddRecord(const Slice& slice);

    private:
    Status AppendToFile(const Slice& data);
    Status FlushBuffer();
    Status WriteUnbuffered(const char* data, size_t size);
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);
    int fd_;
    std::string filename_;
    int block_offset_;

    uint32_t type_crc_[kMaxRecordType + 1];
    char buf_[kLogBufferSize];
    size_t pos_;
};

class LOG_Reader {
    public:
    class Reporter {
        public:
        virtual ~Reporter();
        virtual void Corruption(size_t bytes, const Status& status) = 0;
    };

    LOG_Reader(int fd, std::string filename, Reporter* reporter, bool checksum, uint64_t initial_offset);
    
    LOG_Reader(const LOG_Reader&) = delete;
    LOG_Reader& operator = (const LOG_Reader&) = delete;

    ~LOG_Reader();

    bool ReadRecord(Slice* record, std::string* scratch);
    uint64_t LastRecordOffset();

    private:
    enum {
        kEof = kMaxRecordType + 1,
        kBadRecord = kMaxRecordType + 2
    };

    bool SkipToInitialBlock();
    unsigned int ReadPhysicalRecord(Slice* result);
    void ReportCorruption(uint64_t bytes, const char* reason);
    void ReportDrop(uint64_t bytes, const Status& reason);

    Status FileSkip(uint64_t n);
    Status ReadFromFile(size_t n, Slice* result, char* scratch);

    int fd_;
    std::string filename_;
    Reporter* const reporter_;
    bool const checksum_;
    char* const backing_store_;
    Slice buffer_;
    bool eof_;

    uint64_t last_record_offset_;
    uint64_t end_of_buffer_offset_;
    uint64_t initial_offset_;

    bool resyncing_;
};

} // namespace LOG

} // namespace LSM2LIX

#endif