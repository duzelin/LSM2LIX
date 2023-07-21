#ifndef FILENAME_H
#define FILENAME_H

#include <string>

namespace LSM2LIX {

static const std::string kRocksDbTFileExt = "sst";
static const std::string kTransDbTFileExt = "tsst";

static std::string MakeFileName(uint64_t number, const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%06llu.%s",
           static_cast<unsigned long long>(number), suffix);
  return buf;
}

static std::string MakeFileName(const std::string& path, uint64_t number,
                                const char* suffix) {
  return path + "/" + MakeFileName(number, suffix);
}

static std::string MakeTableFileName(const std::string& path, uint64_t number) {
  return MakeFileName(path, number, kRocksDbTFileExt.c_str());
}

static std::string MakeTransFileName(const std::string& path, uint64_t number) {
  return MakeFileName(path, number, kTransDbTFileExt.c_str());
}

static std::string MakeTableFileName(uint64_t number) {
  return MakeFileName(number, kRocksDbTFileExt.c_str());
}

} // namespace

#endif