#ifndef KEY_INDEXX_H
#define KEY_INNDEX_H

#include <stdint.h>
#include <tuple>

namespace KeyIndex {

#define OFFSET_LENGTH           8   //8B, 64bits
#define SST_BITS                18  //256K 1024MB SSTable files
#define DATABLOCK_BITS          30  //16K 64KB data blocks per SSTable file
#define DATABLOCK_SIZE_BITS     16  //64KB data block size    

class IntKeyAsSlice {
    public:
    IntKeyAsSlice(uint64_t key) : swapped_(__builtin_bswap64(key)) {}

    template <class SliceKind>
    SliceKind as() const {
        return SliceKind(reinterpret_cast<const char*>(&swapped_), sizeof(swapped_));
    }

    private:
    uint64_t swapped_;
};

template <class T>
static T LoadUnaligned(const void* p) {
  T x;
  memcpy(&x, p, sizeof(T));
  return x;
}

template <class SliceKind>
static uint64_t ExtractHead64(const SliceKind& key) {
  switch (key.size()) {
    case 0:
      return 0;
    case 1:
      return static_cast<uint64_t>(key.data()[0]) << 56;
    case 2:
      return static_cast<uint64_t>(
                 __builtin_bswap16(LoadUnaligned<uint16_t>(key.data())))
             << 48;
    case 3:
      return (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data())))
              << 48) |
             (static_cast<uint64_t>(key.data()[2]) << 40);
    case 4:
      return static_cast<uint64_t>(
                 __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
             << 32;

    case 5:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(key.data()[4]) << 24);

    case 6:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data() + 4)))
              << 16);
    case 7:
      return (static_cast<uint64_t>(
                  __builtin_bswap32(LoadUnaligned<uint32_t>(key.data())))
              << 32) |
             (static_cast<uint64_t>(
                  __builtin_bswap16(LoadUnaligned<uint16_t>(key.data() + 4)))
              << 16) |
             (static_cast<uint64_t>(key.data()[6]) << 8);
    default:
      return __builtin_bswap64(LoadUnaligned<uint64_t>(key.data()));
  }
}

class BlockHandleAsOffset {
    public:
    BlockHandleAsOffset(uint64_t filenum, uint64_t offset, uint64_t size) {
        offset_[0] = filenum >> 6;
        offset_[1] = ((filenum & 0x3F) << 2) | (offset >> 12);
        offset_[2] = (offset & 0xFFF) >> 4;
        offset_[3] = ((offset & 0xF) << 4) | (size >> 8);
        offset_[4] = size & 0xFF; 
    }
    std::tuple<uint64_t, uint64_t, uint64_t> RestoreBlockHandle() {
        uint64_t filenum, offset, size;
        filenum = (offset_[0] << 6) | (offset_[1] >> 2);
        offset = ((offset_[1] & 0x3) << 12) | (offset_[2] << 4) | (offset_[3] >> 4);
        size = ((offset_[3] & 0xF) << 8) | (offset_[4]);
        return std::tuple<uint64_t, uint64_t, uint64_t>(filenum, offset, size);
    }

    private:
    unsigned char offset_[OFFSET_LENGTH];
};

// inline void BlockHandleToOffset(uint64_t filenum, uint64_t offset, uint64_t size, char* s) {
//     s[0] = filenum >> 6;
//     s[1] = ((filenum & 0x3F) << 2) | (offset >> 12);
//     s[2] = (offset & 0xFFF) >> 4;
//     s[3] = ((offset & 0xF) << 4) | (size >> 8);
//     s[4] = size & 0xFF; 
// }

// inline void OffsetToBlockHandle(const char* s, uint64_t* filenum, uint64_t* offset, uint64_t* size) {
//     *filenum = (s[0] << 6) | (s[1] >> 2);
//     *offset = ((s[1] & 0x3) << 12) | (s[2] << 4) | (s[3] >> 4);
//     *size = ((s[3] & 0xF) << 8) | (s[4]);
// }

inline void BlockHandleToOffset(uint64_t filenum, uint64_t offset, uint64_t size, char result_value[OFFSET_LENGTH]) {
  uint64_t* result_uint = reinterpret_cast<uint64_t*>(result_value);
  *result_uint = (( filenum & 0x3FFFF ) << ( DATABLOCK_BITS + DATABLOCK_SIZE_BITS ))
                  | (( offset & 0x3FFFFFFF ) << DATABLOCK_SIZE_BITS )
                  | ( size & 0xFFFF );
}

inline void OffsetToBlockHandle(char input_value[OFFSET_LENGTH], uint64_t* filenum, uint64_t* offset, uint64_t* size) {
  uint64_t* input_uint = reinterpret_cast<uint64_t*>(input_value);
  *filenum = *input_uint >> ( DATABLOCK_BITS + DATABLOCK_SIZE_BITS );
  *offset = ( *input_uint >> DATABLOCK_SIZE_BITS ) & 0x3FFFFFFF;
  *size = *input_uint & 0xFFFF;
}

}
#endif