#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  SetPrevPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}
page_id_t LeafPage::GetPrevPageId() const {
  return  prev_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

void LeafPage::SetPrevPageId(page_id_t prev_page_id) {
  prev_page_id_ = prev_page_id;
  if (prev_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  /*-----UPDATE-------
   *这个故事告诉我们没事别用unsigned
   *0-1 = max_uint32了
   */
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (KM.CompareKeys(KeyAt(mid), key) < 0) {
      left = mid + 1;
    }
    else if(KM.CompareKeys(KeyAt(mid),key)>0) {
      right = mid - 1;
    }
    else {
      return mid;
    }
  }
  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  /*------UPDATE-----
   *那我要怎么使size比MaxSize大呢？？？？
  if (GetSize() >= GetMaxSize()) {
    LOG(INFO) << "Fatal error";
    return -1;
  }
  */
  for(int i = GetSize() - 1 ; i >= 0; i--) {
    if (KM.CompareKeys(KeyAt(i), key) > 0) {
      PairCopy(KeyAt(i + 1), KeyAt(i), 1);
    }
    else {
      SetKeyAt(i + 1, key);
      SetValueAt(i + 1, value);
      SetSize(GetSize() + 1);
      return GetSize();
    }
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(GetSize() + 1);
  return GetSize();
  //不存在key插入index=0的情况，因为key小于key[0]就不会插入这一页
  //吗？ 万一新插入一个key比整个树的key都要小呢？
  //好像是要插入最左page的第一个
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int mid = GetSize() / 2;
  //recipient->SetSize(recipient->GetSize() + mid); --bug-- 数值错误，并且在CopyN里面已经加了
  recipient->CopyNFrom(this, GetSize() - GetSize() / 2, 1); // split 一定是往右的
  SetSize(GetSize() / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size,int index) {
  auto src_page = reinterpret_cast<LeafPage *>(src);
  // 或许可以优化，一口气copy完
  if (!index) {  // 从index=0的节点copy
    for (int i = GetSize()-1; i>=0; i--) { // 整体右移size
      PairCopy(PairPtrAt(i+size), PairPtrAt(i),1);
    }
    PairCopy(PairPtrAt(0),src_page->PairPtrAt(src_page->GetSize()-size),size); // copy size个元素到最前面
  }
  else PairCopy(PairPtrAt(GetSize()), src_page->PairPtrAt(src_page->GetSize() - size), size);
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    value = ValueAt(index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    for (uint32_t i = index; i < GetSize() - 1; ++i) {
      PairCopy(KeyAt(i), KeyAt(i + 1), 1);
    }
    SetSize(GetSize() - 1);
    return GetSize();
  }
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient, int index) {
  recipient->CopyNFrom(this, GetSize(),index);

  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  for (uint32_t i = 1; i < GetSize(); ++i) {
    PairCopy(KeyAt(i - 1), KeyAt(i), 1);
  }
  SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  // 用copy来删除最后的尾对
  PairCopy(KeyAt(GetSize() - 1), KeyAt(GetSize()), 1);
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  for(int i = GetSize() - 1; i >= 0; i--) {
    PairCopy(KeyAt(i + 1), KeyAt(i), 1);
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(GetSize() + 1);
}