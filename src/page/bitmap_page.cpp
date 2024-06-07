#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
  * TODO: Student Implement
  */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  uint32_t valid_page_index;
  for (valid_page_index = 0; valid_page_index < GetMaxSupportedSize();valid_page_index++) {
    if(IsPageFree(valid_page_index)) {
      uint32_t byte_index = valid_page_index/8;
      uint32_t bit_index = valid_page_index%8;
      bytes[valid_page_index/8] |= (1 << (7-valid_page_index% 8) );
      page_offset = valid_page_index;
      return true;
    }
  }
}


/**
  * TODO: Student Implement
  */

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  if(IsPageFree(page_offset)) {
    return false;
  }
  bytes[page_offset/8] &= ~(1 << (7-page_offset% 8) );
  return true;
}

/**
  * TODO: Student Implement
  */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    return IsPageFreeLow(page_offset/8,page_offset%8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << (7-bit_index))) == 0;
}



template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;