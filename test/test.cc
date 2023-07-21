#include "rocksdb/sst_file_reader.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/options.h"

#include <cinttypes>
#include <string>
#include <iostream>

#include "reader.h"
#include "compact_files_to_LIX.h"
#include "coding.h"
#include "key_index.h"
#include "LSM2LIX.h"


using ROCKSDB_NAMESPACE::SstFileWriter;
using ROCKSDB_NAMESPACE::SstFileReader;
using ROCKSDB_NAMESPACE::Options;
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

Options options_;
EnvOptions soptions_;
std::string file_path = "/home/dzl/sst_file1.sst";
std::string kDBPath = "/tmp/LSM2LIX";
std::string kLSMPath = "/tmp/LSM2LIX/lsm";
std::string kLIXPath = "/tmp/LSM2LIX/lix";

void CreateFile(const std::string& file_name) {
    SstFileWriter writer(soptions_, options_);
    writer.Open(file_name);
    for (uint64_t i = 1000000; i < 1000999; i++) {
      writer.Put(std::to_string(i), std::to_string(i));
    }
    writer.Finish();
}

void CheckFile(const std::string& file_name) {
    ReadOptions ropts;
    SstFileReader reader(options_);
    reader.Open(file_name);
    reader.VerifyChecksum();
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));

    //iter->Seek(std::to_string(0));
    iter->SeekToFirst();
    iter->Next();
    //std::cout << iter->status().getState() << std::endl;
    if (iter->Valid()) {
        std::cout << "Iter is valid." << std::endl;
    }
    std::pair<uint64_t, uint64_t> index_handle = reader.GetInnermostIndex(iter.get());
    std::cout << index_handle.first << std::endl;
    std::cout << index_handle.second << std::endl;
}

void CheckSST_TEST() {
    CreateFile(file_path);
    CheckFile(file_path);
}

void ReadSSToutside_TEST() {
    CreateFile(file_path);
    CheckFile(file_path);
    std::string result;
    LSM2LIX::Reader reader;
    LSM2LIX::BlockHandle handle = {.offset_ = 0, .size_ = 293}; 
    reader.SetSSTFileName(file_path);
    reader.ReadBlockContents(handle);
    reader.Get(std::to_string(1), &result);
    std::cout << result << std::endl;
}

void ReadSST_TEST() {
    CreateFile(file_path);
    ReadOptions ropts;
    SstFileReader reader(options_);
    reader.Open(file_path);
    reader.VerifyChecksum();
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));

    iter->SeekToFirst();
    while (iter->Valid()) {
        std::pair<uint64_t, uint64_t> index_handle = reader.GetInnermostIndex(iter.get());
        uint64_t k = KeyIndex::ExtractHead64(iter->key());
        std::cout << iter->key().data() << std::endl;
        std::cout << iter->key().size() << std::endl;
        std::cout << k << std::endl;
        iter->Next();
    }
}

void LSM2LIX_TEST() {
    LSM2LIX::LSM2LIX* db;
    LSM2LIX::LSM2LIX::Open(kDBPath, &db);
    for (uint64_t i = 1000; i < 5000000; i++) {
        db->Put(std::to_string(i), std::string(500, 'a' + (i % 26)));
    }
    uint64_t err_count = 0;
    for (uint64_t i = 1000; i < 50000; i++) {
        std::string value;
        LSM2LIX::Status s = db->Get(std::to_string(i), &value);
        if (!s.ok()) {
            err_count += 1;
        }
    }
    printf("Err count: %d\n", err_count);
    delete db;
}

/*
void DetachSST_TEST() {
    ColumnFamilyOptions coptions;
    coptions.max_bytes_for_level_base = 64 << 20;
    coptions.num_levels = 1;
    coptions.target_file_size_base = 16 << 20;
    coptions.write_buffer_size = 16 << 20;
    DBOptions doptions;
    doptions.create_if_missing = true;
    Options options(doptions, coptions);
    float full_pec = 0.9;
    uint64_t threshold = options.max_bytes_for_level_base;
    options.listeners.emplace_back(new LSM2LIX::LSM2LIX_Mover(coptions.num_levels, threshold, options, this));
    

    DB* db;
    Status s = DB::Open(options, kDBPath, &db);
    std::vector<ColumnFamilyDescriptor> column_families;
    for (int i = 0; i < 4; i++) {
        column_families.push_back(ColumnFamilyDescriptor("cf" + std::to_string(i), coptions));
    }
    std::vector<ColumnFamilyHandle*> handles;
    db->CreateColumnFamilies(column_families, &handles);

    for (int i = 1000; i < 1000000; i++) {
        db->Put(WriteOptions(), handles[0], std::to_string(i), std::string(500, 'a' + (i % 26)));
    }

    for (auto handle : handles) {
        s = db->DestroyColumnFamilyHandle(handle);
        assert(s.ok());
    }
    delete db;
}
*/
int main(){
    //DetachSST_TEST();
    CheckSST_TEST();
    return 0;
}