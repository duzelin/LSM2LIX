#include "compact_files_to_LIX.h"

#include <cstdio>
#include <cassert>

#include "status.h"
#include "reader.h"
#include "coding.h"
#include "key_index.h"
#include "filename.h"

namespace LSM2LIX {

void LSM2LIX_Mover::OnCompactionCompleted(DB* db, const CompactionJobInfo& info) {
    uint64_t old_id, new_id, total_size;
    std::string old_name, old_path;
    ROCKSDB_NAMESPACE::Status s;
    if (info.output_level == bottom_level_) {
        new_id = counter_.fetch_add(1);
        s = db->SelectTransFile(bottom_level_size_threshold_, &total_size, info.cf_id, &old_id, &old_name, &old_path, new_id, false);
        while (total_size > bottom_level_size_threshold_) {
        // while (old_id != std::numeric_limits<uint64_t>::max()) {
            if (old_id != std::numeric_limits<uint64_t>::max()) {
                std::string filename = MakeTableFileName(old_path, old_id);
                std::vector<tl::pg::Record> pairs;
                char* offset_values = GetTreeLineIndexPair(filename, new_id, options_, rdoptions_, &pairs);
                Status l2ls = lsm2lix_db_->BatchUpdate_LIX(pairs, old_id, new_id, info.cf_id, /*redo*/false);
                std::cout << info.cf_id << std::endl;
                delete offset_values; // release the temp buffer.
                s = db->DetachSSTFile(info.cf_id, old_id);
                new_id = counter_.fetch_add(1);
                if (!s.ok()) {
                    printf("[Mover] : Detach fiie failed. \n");
                }
            }
            s = db->SelectTransFile(bottom_level_size_threshold_, &total_size, info.cf_id, &old_id, &old_name, &old_path, new_id, false);
        }
    }   
}

char* LSM2LIX_Mover::GetTreeLineIndexPair(std::string& filename, uint64_t filenum, Options& options, ReadOptions& rdoptions, std::vector<tl::pg::Record>* pairs) {
    pairs->clear();

    SstFileReader reader(options);
    reader.Open(filename);
    reader.VerifyChecksum();
    auto table_properties = reader.GetTableProperties();
    uint64_t num_entries = table_properties->num_entries;
    std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> iter(reader.NewIterator(rdoptions));
    iter->SeekToFirst();
    char* offset_values = new char[OFFSET_LENGTH * num_entries];
    uint64_t processed_entries = 0;
    while (iter->Valid()) {
        std::pair<uint64_t, uint64_t> index_handle = reader.GetInnermostIndex(iter.get());
        uint64_t k = KeyIndex::ExtractHead64(iter->key());
        char* offset_value = offset_values + processed_entries * OFFSET_LENGTH;
        KeyIndex::BlockHandleToOffset(filenum, index_handle.first, index_handle.second, offset_value);
        pairs->emplace_back(k, tl::Slice(offset_value, OFFSET_LENGTH));
        processed_entries += 1;
        iter->Next();
    }
    assert(processed_entries == num_entries);
    return offset_values;
}

} // namespace