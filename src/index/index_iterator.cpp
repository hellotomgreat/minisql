#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  page_id_t page_id = current_page_id;
  buffer_pool_manager->FetchPage(page_id);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
  return leaf_page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  page_id_t page_id = current_page_id;
  //buffer_pool_manager->FetchPage(page_id); ----bug--- 意义不明的一句
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
  // 还在当前页
  if (item_index + 1 < page->GetSize()) {
    item_index++;
    buffer_pool_manager->UnpinPage(current_page_id, true);
    return *this;
  } else {
    // 已经到达当前页的末尾，需要遍历下一页
    page_id_t next_page_id = leaf_page->GetNextPageId();
    item_index = 0;
    buffer_pool_manager->UnpinPage(current_page_id, true);
    current_page_id = next_page_id;
    if (next_page_id == INVALID_PAGE_ID) {
      // 已经遍历完所有页
      return *this;
    }
    Page *next_page = buffer_pool_manager->FetchPage(current_page_id);
    LeafPage *next_leaf_page = reinterpret_cast<LeafPage *>(next_page->GetData());
    page = next_leaf_page;
    buffer_pool_manager->UnpinPage(current_page_id, true);
    return *this;
  }
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}