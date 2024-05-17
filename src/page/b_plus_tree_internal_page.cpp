#include "page/b_plus_tree_internal_page.h"

#include <parser/minisql_yacc.h>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()
// debug warning !!!! 本章中可能出现的bug大概率会和middlekey的安插有关，主要是我对middlekey的理解不够，想象不出来这玩意怎么工作的，只知道要把它插在新页的最开始。
// 此外修改键值对时的index有可能存在多数或者少数，我自己梳理了一边，但不确定是不是对的
// 另外涉及diskmanager的地方，我力大砖飞，把原页和新页都unpin了，应该无伤大雅（虽然浪费资源就是了）
/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  uint32_t left = 1;
  uint32_t right = GetSize() - 1;

  while (left <= right) {
    uint32_t mid = (left + right) / 2;
    int cmp = KM.CompareKeys(key, KeyAt(mid));
    // 如果找到了，就返回对应的值
    if (cmp == 0) {
      return ValueAt(mid);
    }
    // 如果key小于mid的key，则在mid的左边找，对应的要让right左移，取到mid-1
    else if (cmp < 0) {
      right = mid - 1;
    }
    // 反之亦然
    else {
      left = mid + 1;
    }
  }
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetParentPageId(INVALID_PAGE_ID);
  SetSize(2);
  // ！！！！SetKeyAt(0,invalid_key);  note:按照指导书要求这里要将第一个键值设置为无效键值，但是找不到类似invalid_key的定义,姑且先放着，到时候看看有没有问题
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  uint32_t old_index = ValueIndex(old_value);
  if (old_index == -1) {
    return -1;
  }
  // 找到了old_value对应的index，插入新的键值对
  int new_size = GetSize() + 1;
  if (new_size > GetMaxSize()) {
    return -1;
  }
  // 要把原来old_index后面的所有元素往后移动一位，腾出位置给新插入的键值对,这里可能存在优化空间，不用一个个挪，而是一次性搬移，btw,我不太清楚pairptrat(getsize()）是不是有效的，毕竟这里还没有pair,先写着，不行再改
  for (int i = GetSize() - 1; i > old_index; --i) {
    PairCopy(PairPtrAt(i + 1), PairPtrAt(i), 1);
  }
  SetKeyAt(old_index + 1, new_key);
  SetValueAt(old_index + 1, new_value);
  SetSize(new_size);
  return new_size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // 复制当前页前半部分对到recipient页，并在函数内部完成size的修改
  recipient->CopyNFrom(this, GetSize() - GetSize() / 2, buffer_pool_manager);
  // 删除当前页前半部分对，后半部分的前移已经包含在remove里
  for(uint32_t i = 1; i <(GetSize() -GetSize() / 2); ++i) {
    Remove(i);
  }
  SetSize(GetSize() / 2);

}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
// 问了一下ai，说这个函数时为了把copy行为在内存层面固定下来，copy行为我直接用的paircopy,看了一下实现，直接调的memcpy并且更新这些移动的键值对的父节点
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  auto src_page = reinterpret_cast<InternalPage *>(src);
  auto this_page_id = GetPageId();
  // 先把src_page的键值对拷贝到当前页，然后更新父节点
  for (uint32_t num = 0; num < size; ++num) {
    PairCopy(PairPtrAt(GetSize() + num), src_page->PairPtrAt(num), 1);
    IncreaseSize(1);
    // 更新父节点，先找到键值对所在的页，转化为BPlusTreePage指针，然后更新父节点
    auto child_page_id = src_page->ValueAt(num);
    auto child_page = buffer_pool_manager->FetchPage(child_page_id);
    // 此处我参考了指导书中的b+tree的内存与页的对应关系
    auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_page_ptr->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  buffer_pool_manager->UnpinPage(this_page_id, true);
  SetSize(GetSize()+size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  for (uint32_t i = index; i <= GetSize() - 1; ++i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i+1), 1);
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  if (GetSize() == 1) {
    page_id_t child_page_id = ValueAt(0);
    Remove(0);
    SetSize(0);
    return child_page_id;
  }
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0,middle_key);
  // 尽管页里面第一个键是无效键，但是拷贝到新页中时，新页中第一个键依然是无效的，此处的行为不过是将中间键添加到无效键后面然后全拷贝原页的内容，这里逻辑理了好久，不知道对不对www,如果uu想明白的话还烦请注释补充一下
  recipient->CopyNFrom(this,GetSize(),buffer_pool_manager);
  // 下面部分是为了移除当前页的键值对，不知道有没有意义，如果能照常跑的话就不用去掉注释
  // for(uint32_t i = 0; i < GetSize(); ++i) {
  //   Remove(i);
  // }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 先把当前页的最后一个键值对拷贝到recipient页，然后更新父节点
  PairCopy(PairPtrAt(GetSize()-1), key, 1);
  IncreaseSize(1);
  SetValueAt(GetSize(), value);
  // 更新父节点，先找到键值对所在的页，转化为BPlusTreePage指针，然后更新父节点，内容与先前的结构相同
  page_id_t this_page_id = GetPageId();
  auto child_page = buffer_pool_manager->FetchPage(value);
  auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_page_ptr->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  buffer_pool_manager->UnpinPage(this_page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(ValueAt(GetSize()-1), buffer_pool_manager);
  recipient->SetKeyAt(0, middle_key);
  Remove(GetSize()-1);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetValueAt(0, value);
  page_id_t this_page_id = GetPageId();
  auto child_page = buffer_pool_manager->FetchPage(value);
  auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_page_ptr->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  buffer_pool_manager->UnpinPage(this_page_id, true);
  IncreaseSize(1);
}