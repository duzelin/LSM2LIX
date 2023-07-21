#include "LSM2LIX.h"

#include <algorithm>
#include <filesystem>
#include <chrono>
#include <iostream>
#include <fstream>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>
#include <fcntl.h>
#include <ctime>
#include <cmath>

#include "reader.h"
#include "compact_files_to_LIX.h"
#include "key_index.h"
#include "filename.h"
#include "log_table.h"
#include "coding.h"

// #define TIMING 1

namespace LSM2LIX {

Status LSM2LIX::Open(std::string& DB_path, LSM2LIX** db_out) {
    Status status;
    LSM2LIX* db = new LSM2LIX(DB_path);
    if (status.ok()) {
        *db_out = db;
    } else {
        delete db;
    }
    return status;
}

LSM2LIX::LSM2LIX(std::string& DB_path) {
    DB_path_ = DB_path;
    LSM_path_ = DB_path + "/" + LSM_dir;
    LIX_path_ = DB_path + "/" + LIX_dir;

    bool empty = true;
    if (std::filesystem::exists(DB_path) && std::filesystem::is_directory(DB_path) && !std::filesystem::is_empty(DB_path)) {
        empty = false;
    }
    if (empty) {
        std::filesystem::create_directory(DB_path);
    }

    RecoverStageI();
    
    // Init TreeLine
    tl::pg::PageGroupedDBOptions tloptions;
    tloptions.use_memory_based_io = false;
    tloptions.bypass_cache = true;
    tloptions.use_pgm_builder = true;
    tloptions.forecasting.use_insert_forecasting = false;
    tloptions.disable_overflow_creation = true;
    tloptions.num_bg_threads = 0;
    tl::pg::PageGroupedDBStats::RunOnGlobal([](auto& global_stats) { global_stats.Reset(); });
    tl::Status tls = tl::pg::PageGroupedDB::Open(tloptions, LIX_path_, &tldb_);

    // Init LSM-forest
    // TODO: disable compaction job
    ColumnFamilyOptions coptions;
    //coptions.disable_auto_compactions = true; // Disable compaction, after recoverying reset it.
    coptions.max_bytes_for_level_base = 640 << 20;
    coptions.num_levels = 1;
    coptions.target_file_size_base = 64 << 20;
    coptions.write_buffer_size = 64 << 20;
    BlockBasedTableOptions table_options;
    table_options.block_size_deviation = 50;
    table_options.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
    coptions.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DBOptions doptions;
    doptions.create_if_missing = true;
    doptions.create_missing_column_families = true;
    doptions.max_background_jobs = 8;
    doptions.use_direct_io_for_flush_and_compaction = true;
    doptions.use_direct_reads = true;
    Options options(doptions, coptions);
    options_ = options;
    float full_pec = 0.9;
    uint64_t threshold = full_pec * options.max_bytes_for_level_base * pow(static_cast<int>(options.max_bytes_for_level_multiplier), (options.num_levels - 1));
    uint64_t SST_NUM = TransID2SSTMeta_.empty() ? 0 : TransID2SSTMeta_.rbegin()->first;
    options.listeners.emplace_back(new LSM2LIX_Mover(coptions.num_levels, threshold, options, this, SST_NUM)); // By defalut, the last item in the std::map has the largest key.

    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, coptions));
    for (int i = 0; i < ColumnFamilyCnt; i++) {
        column_families.push_back(ColumnFamilyDescriptor("cf" + std::to_string(i), coptions));
    }
    ROCKSDB_NAMESPACE::Status s = DB::Open(options, LSM_path_, column_families, &handles_, &db_);

    if (!std::filesystem::exists(LIX_path_)) {
        bulkload_ = true;
    } 
    // else {
    //     {
    //         std::ifstream ifs("/tmp/LSM2LIX/TransFile_dir_.ser");
    //         boost::archive::text_iarchive ia(ifs);
    //         ia & TransFile_dir_;
    //     }
    //     {
    //         std::ifstream ifs("/tmp/LSM2LIX/TransId2SstId_.ser");
    //         boost::archive::text_iarchive ia(ifs);
    //         ia & TransId2SstId_;
    //     }
    //     {
    //         std::ifstream ifs("/tmp/LSM2LIX/TransId2DirId_.ser");
    //         boost::archive::text_iarchive ia(ifs);
    //         ia & TransId2DirId_;
    //     }
    // }

    // datablock_reader_.AllocateBuf();
    RecoverStageII();
    //db_->SetOptions({{"disable_auto_compactions", "false"}}); // enable the auto compaction
}

LSM2LIX::~LSM2LIX(){
    delete db_;
    delete tldb_;
    delete mLogWriter_;
    ::close(mlog_fd_);
    // {
    //     std::ofstream ofs("/tmp/LSM2LIX/TransFile_dir_.ser");
    //     boost::archive::text_oarchive oa(ofs);
    //     oa & TransFile_dir_;
    // }
    // {
    //     std::ofstream ofs("/tmp/LSM2LIX/TransId2SstId_.ser");
    //     boost::archive::text_oarchive oa(ofs);
    //     oa & TransId2SstId_;
    // }
    // {
    //     std::ofstream ofs("/tmp/LSM2LIX/TransId2DirId_.ser");
    //     boost::archive::text_oarchive oa(ofs);
    //     oa & TransId2DirId_;
    // }
}

Status LSM2LIX::Put(const ROCKSDB_NAMESPACE::Slice& key, const ROCKSDB_NAMESPACE::Slice& value) {
    Status status;
    uint64_t key_num = KeyIndex::ExtractHead64(key);
    uint32_t handle_num = DispatchRequest(key_num, ColumnFamilyCnt);
    ROCKSDB_NAMESPACE::Status s = db_->Put(wopts_, handles_[handle_num] ,key, value);
    return status;
}

Status LSM2LIX::Get(const ROCKSDB_NAMESPACE::Slice& key, std::string* value) {
#ifdef TIMING
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::microseconds;

    std::chrono::microseconds us_lsm, us_lix, us_block0, us_block1, us_get;
#endif
    Status status;
    uint64_t key_num = KeyIndex::ExtractHead64(key);
    uint32_t handle_num = DispatchRequest(key_num, ColumnFamilyCnt);
#ifdef TIMING
    auto t0 = high_resolution_clock::now();
#endif
    ROCKSDB_NAMESPACE::Status s = db_->Get(ropts_, handles_[handle_num], key, value);
#ifdef TIMING
    auto t1 = high_resolution_clock::now();
    us_lsm = duration_cast<microseconds>(t1 - t0);
#endif
    if (s.IsNotFound()) {
        std::string offset_value;
        uint64_t filenum, offset, size;
        std::string filename, filename_old;
#ifdef TIMING
        auto t2 = high_resolution_clock::now();
#endif
        tl::Status tls = tldb_->Get(key_num, &offset_value);
#ifdef TIMING
        auto t3 = high_resolution_clock::now();
        us_lix = duration_cast<microseconds>(t3 - t2);
#endif
        if (tls.IsNotFound()) {
            return Status::NotFound("Key is not found.");
        }
        KeyIndex::OffsetToBlockHandle(const_cast<char*>(offset_value.c_str()), &filenum, &offset, &size);
        BlockHandle handle = {.offset_ = offset, .size_ = size};
        Reader datablock_reader;
        datablock_reader.AllocateBuf();
        bool read = false;
        {// lock phase TODO; This lock is tooooo heavy, it should be replaced.
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Find Sst id.
        auto it = TransID2SSTMeta_.find(filenum);
        if (it->second.flag == Detaching) {
            filename_old = MakeTableFileName(LSM_path_, it->second.SST_ID);
            datablock_reader.SetSSTFileName(filename_old);
#ifdef TIMING
            auto t4 = high_resolution_clock::now();
#endif
            status = datablock_reader.ReadBlockContents(handle);
#ifdef TIMING
            auto t5 = high_resolution_clock::now();
            us_block0 = duration_cast<microseconds>(t5 - t4);
#endif
            if (status.IsNotFound()) { // Old SST file name is out-of-date.
                it->second.flag = Normal;
                // Add a record in the mLog
                char record_buf[60];
                uint64_t offset = 0;
                uint64_t record_type = modify;
                EncodeFixed64(record_buf + offset, record_type);
                offset += sizeof(uint64_t);
                EncodeFixed64(record_buf + offset, filenum);
                offset += sizeof(uint64_t);
                EncodeFixed64(record_buf + offset, Normal);
                offset += sizeof(uint64_t);
                mLogWriter_->AddRecord(Slice(record_buf, offset)); 
            } else {
                read = true;
            }
        }
        } // end of lock phase
        if (!read) {
            filename = MakeTransFileName(LSM_path_, filenum);
            datablock_reader.SetSSTFileName(filename);
#ifdef TIMING
            auto t6 = high_resolution_clock::now();
#endif
            status = datablock_reader.ReadBlockContents(handle);
#ifdef TIMING
            auto t7 = high_resolution_clock::now();
            us_block1 = duration_cast<microseconds>(t7 - t6);
#endif
        }
        if (!status.ok()) {
            status = Status::IOError("Data block can not be read.");
        } else {
#ifdef TIMING
            auto t8 = high_resolution_clock::now();
#endif
            status = datablock_reader.Get(key.data(), value);
#ifdef TIMING
            auto t9 = high_resolution_clock::now();
            us_get = duration_cast<microseconds>(t9 - t8);
#endif
            if (!status.ok()) {
                status = Status::NotFound("Key is not found in data block.");
            }
        }
        datablock_reader.FreeBuf();
    }
    return status;
}

Status LSM2LIX::BatchUpdate_LIX(std::vector<tl::pg::Record>& pairs, uint64_t old_id, uint64_t new_id, uint32_t cf_id, bool redo){
    // Todo: RW-Lock
    Status status;
    tl::Status tls;

    char record_buf[60];
    uint64_t offset = 0;
    { // lock phase
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!redo) {
        SSTableMeta stm = {.SST_ID = old_id, .cf_id = cf_id, .smallest_key = pairs.front().first, .largest_key = pairs.back().first, .flag = Transfering};
        TransID2SSTMeta_.emplace(new_id, stm);
        // Add a record in the mLog
        uint64_t record_type = insert;
        EncodeFixed64(record_buf + offset, record_type);
        offset += sizeof(uint64_t);
        EncodeFixed64(record_buf + offset, new_id);
        offset += sizeof(uint64_t);
        EncodeFixed64(record_buf + offset, old_id);
        offset += sizeof(uint64_t);
        EncodeFixed64(record_buf + offset, cf_id);
        offset += sizeof(uint64_t);
        uint64_t smallest_key = pairs.front().first;
        EncodeFixed64(record_buf + offset, smallest_key);
        offset += sizeof(uint64_t);
        uint64_t largest_key = pairs.back().first;
        EncodeFixed64(record_buf + offset, largest_key);
        offset += sizeof(uint64_t);
        EncodeFixed64(record_buf + offset, static_cast<uint64_t>(Transfering));
        offset += sizeof(uint64_t);
        mLogWriter_->AddRecord(Slice(record_buf, offset));
    }
    if (bulkload_) {
        tls = tldb_->BulkLoad(pairs);
        bulkload_ = false;
        if (!tls.ok()) {
            status = Status::IOError("Batch Load Failed.");
        }
        return status;
    }
    } // lock phase 
    tls = tldb_->PutBatch(pairs);
    if (!tls.ok()) {
        status = Status::IOError("Batch Load Failed.");
    }
    { // lock phase
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = TransID2SSTMeta_.find(new_id);
    it->second.flag = Detaching;
    // Add a record in the mLog
    uint64_t offset = 0;
    uint64_t record_type = modify;
    EncodeFixed64(record_buf + offset, record_type);
    offset += sizeof(uint64_t);
    EncodeFixed64(record_buf + offset, new_id);
    offset += sizeof(uint64_t);
    EncodeFixed64(record_buf + offset, Detaching);
    offset += sizeof(uint64_t);
    mLogWriter_->AddRecord(Slice(record_buf, offset));
    } // lock phase
    return status;
}

void LSM2LIX::Set_mLogWriter(LOG::LOG_Writer* mLogWriter) {
    mLogWriter_ = mLogWriter;
}

Status LSM2LIX::RecoverStageI() {
    Status status;
    std::vector<std::string> mLOG_list, tLOG_list;
    // First, we recover the table from the mLOG
    status = RecovermLogFile();
    // ExtractTodoList(&todolist_);
    // status = RecovertLogFile(&todolist_);

    detachlist_.clear();
    todolist_.clear();
    std::map<uint64_t, SSTableMeta>::iterator it;
    for (it = TransID2SSTMeta_.begin(); it != TransID2SSTMeta_.end(); it++) {
        if (it->second.flag == Detaching) { // Manually rename to prevent the LSM-tree deleting the old SSTable file
            std::string old_name = MakeTableFileName(LSM_path_, it->second.SST_ID);
            std::string new_name = MakeTransFileName(LSM_path_, it->first);
            if (std::rename(old_name.c_str(), new_name.c_str()) == 0) { // The SSTable has not been renamed before recovery
                detachlist_.emplace_back(it->first);
            }
        } else if (it->second.flag == Transfering) {
            todolist_.emplace_back(it->first);
        }
    }
    return status;
}

Status LSM2LIX::RecoverStageII() {
    Status status;

    // Rewrite the mLog
    time_t now;
    time(&now);
    tm info;
    char time_stamp[80];
    localtime_r(&now, &info);
    strftime(time_stamp, 80, "%Y-%m-%d-%H:%M:%S", &info);
    std::string mlog_path = DB_path_ + "/" + time_stamp + "." + mLOG_suffix;
    mlog_fd_ = ::open(mlog_path.c_str(), O_CREAT | O_RDWR);
    mLogWriter_ = new LOG::LOG_Writer(mlog_fd_, mlog_path);
    char record_buf[60];
    uint64_t offset = 0;
    std::map<uint64_t, SSTableMeta>::iterator it;
    for (it = TransID2SSTMeta_.begin(); it != TransID2SSTMeta_.end(); it++) {
        uint64_t record_type = insert;
        EncodeFixed64(record_buf + offset, record_type);
        offset += sizeof(uint64_t);
        uint64_t SST_NUM = it->first;
        EncodeFixed64(record_buf + offset, SST_NUM);
        offset += sizeof(uint64_t);
        uint64_t SST_ID = it->second.SST_ID;
        EncodeFixed64(record_buf + offset, SST_ID);
        offset += sizeof(uint64_t);
        uint64_t cf_id = it->second.cf_id;
        EncodeFixed64(record_buf + offset, cf_id);
        offset += sizeof(uint64_t);
        uint64_t smallest_key = it->second.smallest_key;
        EncodeFixed64(record_buf + offset, smallest_key);
        offset += sizeof(uint64_t);
        uint64_t largest_key = it->second.largest_key;
        EncodeFixed64(record_buf + offset, largest_key);
        offset += sizeof(uint64_t);
        uint64_t flag = it->second.flag;
        EncodeFixed64(record_buf + offset, flag);
        offset += sizeof(uint64_t);
        status = mLogWriter_->AddRecord(Slice(record_buf, offset));
    }
    //TODO: the stale mLog file can be removed at here

    // Replay the todolist
    for (uint64_t SST_NUM : todolist_) {
        std::map<uint64_t, SSTableMeta>::iterator it = TransID2SSTMeta_.find(SST_NUM);
        uint32_t cf_id = static_cast<uint32_t>(it->second.cf_id);
        uint64_t old_id = it->second.SST_ID;
        uint64_t total_size = 0;
        std::string old_name, old_path;
        db_->SelectTransFile(0, &total_size, cf_id, &old_id, &old_name, &old_path, SST_NUM, /*force*/true);
        std::string filename = MakeTableFileName(old_path, old_id);
        std::vector<tl::pg::Record> pairs;
        char* offset_values = LSM2LIX_Mover::GetTreeLineIndexPair(filename, SST_NUM, options_, ropts_, &pairs);
        BatchUpdate_LIX(pairs, old_id, SST_NUM, cf_id, /*redo*/true);
        detachlist_.emplace_back(SST_NUM);
        delete offset_values;
    }

    // Replay the detachlist
    for (uint64_t SST_NUM : detachlist_) {
        std::map<uint64_t, SSTableMeta>::iterator it = TransID2SSTMeta_.find(SST_NUM);
        uint32_t cf_id = static_cast<uint32_t>(it->second.cf_id);
        uint64_t old_id = it->second.SST_ID;
        db_->DetachSSTFile(cf_id, old_id);
    }
    return status;
}

Status LSM2LIX::RecovermLogFile() {
    Status status;
    class LogReporter : public LOG::LOG_Reader::Reporter {
        public:
        LogReporter() : dropped_bytes_(0) {}
        void Corruption(size_t bytes, const Status& s) override {
            dropped_bytes_ += bytes;
            message_.append(s.ToString());
        }
        size_t dropped_bytes_;
        std::string message_;
    };

    // Open the log file
    std::vector<std::string> log_path;
    std::string fname;
    LOG::LoadFileList(DB_path_, &log_path, mLOG_type);
    if (log_path.empty()) { // no mLog file
        return status;
    } else {
        fname = DB_path_ + "/" + log_path.back();
    }
    int fd = ::open(fname.c_str(), O_RDONLY);
    LogReporter reporter;
    LOG::LOG_Reader reader(fd, fname, &reporter, true, 0);
    std::cout << "Recovering log:" << fname << std::endl;

    // Read the Metatable and rebuild it.
    TransID2SSTMeta_.clear();
    std::string scratch;
    Slice record;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        uint64_t offset = 0;
        uint64_t record_type = DecodeFixed64(record.data());
        switch(record_type) {
            case insert:
            {
            offset += sizeof(uint64_t);
            uint64_t SST_NUM = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t SST_ID = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t cf_id = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t smallest_key = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t largest_key = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t flag = DecodeFixed64(record.data() + offset);
            SSTableMeta stm = {.SST_ID = SST_ID, .cf_id = cf_id, .smallest_key = smallest_key, .largest_key = largest_key, .flag = flag};
            TransID2SSTMeta_.emplace(SST_NUM, stm);
            }
            break;
            case modify:
            {
            offset += sizeof(uint64_t);
            uint64_t SST_NUM = DecodeFixed64(record.data() + offset);
            offset += sizeof(uint64_t);
            uint64_t flag = DecodeFixed64(record.data() + offset);
            TransID2SSTMeta_.find(SST_NUM)->second.flag = flag;
            }
            break;
            case remove:
            {
            offset += sizeof(uint64_t);
            uint64_t SST_NUM = DecodeFixed64(record.data() + offset);
            TransID2SSTMeta_.erase(SST_NUM);
            }
            break;
        }
    }
    ::close(fd);
    return status;
}

Status LSM2LIX::RecovertLogFile(std::vector<uint64_t>* todolist) {
    Status status;
    class LogReporter : public LOG::LOG_Reader::Reporter {
        public:
        LogReporter() : dropped_bytes_(0) {}
        void Corruption(size_t bytes, const Status& s) override {
            dropped_bytes_ += bytes;
            message_.append(s.ToString());
        }
        size_t dropped_bytes_;
        std::string message_;
    };

    // Open the log file
    std::vector<std::string> log_path;
    LOG::LoadFileList(DB_path_, &log_path, tLOG_type);
    if (log_path.empty()) { // no mLog file
        return status;
    }
    todolist->clear();
    for (auto& fname : log_path) {
        fname = DB_path_ + "/" + fname;
        int fd = ::open(fname.c_str(), O_RDONLY);
        LogReporter reporter;
        LOG::LOG_Reader reader(fd, fname, &reporter, true, 0);
        std::cout << "Recovering log:" << fname << std::endl;
        
        std::string scratch;
        Slice record;
        uint64_t SST_NUM;
        if (reader.ReadRecord(&record, &scratch)) {
            SST_NUM = DecodeFixed64(record.data());
            todolist->push_back(SST_NUM);
        }

    }
    return status;
}

void LSM2LIX::ExtractTodoList(std::vector<uint64_t>* todolist) {
    todolist->clear();
    std::map<uint64_t, SSTableMeta>::iterator it;
    for (it = TransID2SSTMeta_.begin(); it != TransID2SSTMeta_.end(); it++) {
        if (it->second.flag == Transfering) {
            todolist->emplace_back(it->first);
        }
    }
}

uint32_t LSM2LIX::DispatchRequest(uint64_t key_num, uint32_t tree_num)
{
    uint64_t max_key_range = std::numeric_limits<uint64_t>::max();
    uint64_t tree_key_range = max_key_range / tree_num;
    uint64_t target_tree = key_num / tree_key_range;
    return target_tree;
}

} // namespace