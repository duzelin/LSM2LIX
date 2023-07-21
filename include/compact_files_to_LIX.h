#ifndef COMPACT_H
#define COMPACT_H

#include "LSM2LIX.h"

#include <mutex>
#include <string>
#include <atomic>
#include <cstdint>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"

using ROCKSDB_NAMESPACE::ColumnFamilyMetaData;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::CompactionOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::EventListener;
using ROCKSDB_NAMESPACE::CompactionJobInfo;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::LiveFileMetaData;
using ROCKSDB_NAMESPACE::SstFileMetaData;
using ROCKSDB_NAMESPACE::SstFileReader;
using ROCKSDB_NAMESPACE::Iterator;

using tl::pg::Key;
using tl::Slice;

namespace LSM2LIX {

class LSM2LIX_Mover : public EventListener {
    private:
    int bottom_level_;
    uint64_t bottom_level_size_threshold_;
    Options options_;
    ReadOptions rdoptions_;
    CompactionOptions compact_options_;
    std::atomic<uint64_t> counter_;
    LSM2LIX* lsm2lix_db_;

    public:
    explicit LSM2LIX_Mover(int num_levels, uint64_t bottom_level_size_threshold, Options& options, LSM2LIX* db, uint64_t SST_NUM) {
        bottom_level_ = num_levels;
        bottom_level_size_threshold_ = bottom_level_size_threshold;
        counter_.store(SST_NUM);
        options_ = options;
        lsm2lix_db_ = db;
    }
    static char* GetTreeLineIndexPair(std::string& filename, uint64_t filenum, Options& option, ReadOptions& rdoptions, std::vector<tl::pg::Record>* pairs);
    void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override;
    
};

} // namespcae

#endif