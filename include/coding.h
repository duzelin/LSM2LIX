#ifndef CODING_H
#define CODING_H

#include <cstdint>
#include <cstring>
#include <string>

#include "slice.h"

namespace LSM2LIX {

// Standard Put... routines append to a string
void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint64(std::string* dst, uint64_t value);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool GetVarint32(Slice* input, uint32_t* value);
bool GetVarint64(Slice* input, uint64_t* value);
bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* v);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char* EncodeVarint32(char* dst, uint32_t value);
char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written

inline void EncodeFixed32(char* dst, uint32_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
}

inline void EncodeFixed64(char* dst, uint64_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
  buffer[4] = static_cast<uint8_t>(value >> 32);
  buffer[5] = static_cast<uint8_t>(value >> 40);
  buffer[6] = static_cast<uint8_t>(value >> 48);
  buffer[7] = static_cast<uint8_t>(value >> 56);
}

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t DecodeFixed32(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint32_t>(buffer[0])) |
         (static_cast<uint32_t>(buffer[1]) << 8) |
         (static_cast<uint32_t>(buffer[2]) << 16) |
         (static_cast<uint32_t>(buffer[3]) << 24);
}

inline uint64_t DecodeFixed64(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint64_t>(buffer[0])) |
         (static_cast<uint64_t>(buffer[1]) << 8) |
         (static_cast<uint64_t>(buffer[2]) << 16) |
         (static_cast<uint64_t>(buffer[3]) << 24) |
         (static_cast<uint64_t>(buffer[4]) << 32) |
         (static_cast<uint64_t>(buffer[5]) << 40) |
         (static_cast<uint64_t>(buffer[6]) << 48) |
         (static_cast<uint64_t>(buffer[7]) << 56);
}

inline uint64_t DecodeFixed64(const char* ptr, bool LE) {
  if (LE) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const uint8_t*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

} // namespace LSM2LIX

#endif