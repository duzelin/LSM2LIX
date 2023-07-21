#ifndef LSM2LIX_H
#define LSM2LIX_H

#include <string>
#include <map>
#include <shared_mutex>

#include "rocksdb/db.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "treeline/pg_db.h"
#include "treeline/pg_stats.h"
#include "status.h"
#include "reader.h"
#include "log_table.h"

#define LSM_dir "LSM"
#define LIX_dir "LIX"

#define ColumnFamilyCnt 4

using ROCKSDB_NAMESPACE::SstFileWriter;
using ROCKSDB_NAMESPACE::SstFileReader;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::FilterPolicy;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::EnvOptions;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;

namespace LSM2LIX {

enum Flag {
    Normal = 0,
    Transfering = 1,
    Detaching = 2,
    GCing = 3
};

enum MetaOp {
    insert = 0,
    modify = 1,
    remove = 2
};

struct SSTableMeta {
    uint64_t SST_ID; // The id assigned by LSM-tree
    uint64_t cf_id;
    uint64_t smallest_key;
    uint64_t largest_key;
    uint64_t flag;
};

class LSM2LIX {
    public:
    static Status Open(std::string& DB_path, LSM2LIX** db_out);
    LSM2LIX(std::string& DB_path);
    ~LSM2LIX();

    Status Put(const ROCKSDB_NAMESPACE::Slice& key, const ROCKSDB_NAMESPACE::Slice& value);
    Status Get(const ROCKSDB_NAMESPACE::Slice& key, std::string* value);
    Status BatchUpdate_LIX(std::vector<tl::pg::Record>& pairs, uint64_t old_id, uint64_t new_id, uint32_t cf_id, bool redo = false);
    void Set_mLogWriter(LOG::LOG_Writer* mLogWriter);

    private:

    Status RecoverStageI();
    Status RecoverStageII();
    Status RecovermLogFile();
    Status RecovertLogFile(std::vector<uint64_t>* todolist);
    void ExtractTodoList(std::vector<uint64_t>* todolist);

    uint32_t DispatchRequest(uint64_t key_num, uint32_t tree_num);

    DB* db_;
    std::vector<ColumnFamilyHandle*> handles_;
    Options options_;
    ReadOptions ropts_;
    WriteOptions wopts_;
    tl::pg::PageGroupedDB* tldb_;
    // Reader datablock_reader_;
    // std::map<uint64_t, uint64_t> TransId2SstId_; // new id - old id
    // std::map<uint64_t, uint64_t> TransId2DirId_; // new id - dir id
    // std::vector<std::string> TransFile_dir_;
    std::map<uint64_t, SSTableMeta> TransID2SSTMeta_; // Metatable
    std::string DB_path_;
    std::string LSM_path_;
    std::string LIX_path_;

    mutable std::shared_mutex mutex_;
    bool bulkload_ = false;

    std::vector<uint64_t> todolist_;
    std::vector<uint64_t> detachlist_;
    LOG::LOG_Writer* mLogWriter_;
    int mlog_fd_;
};

} // namespcae

#endif