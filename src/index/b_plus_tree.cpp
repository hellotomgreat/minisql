#include "index/b_plus_tree.h"

#include <bits/stl_tree.h>

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

//#define MY_TEST_
/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // 先给root节点调一个页, --de--不用的。。 startNewTree里面会做的工作.
  root_page_id_ = INVALID_PAGE_ID;
  /*---UPDATE----
   *leaf max size internal max size是需要算出来的，不会传入的.
   */
#ifndef MY_TEST_
  leaf_max_size_ = (PAGE_SIZE-32)/ (KM.GetKeySize()+sizeof(RowId)) - 1; //最多能容纳的键值对
  internal_max_size_ = (PAGE_SIZE-32)/(KM.GetKeySize()+sizeof(page_id_t)) - 1; // 留一位split
#endif
#ifdef MY_TEST_
  leaf_max_size_ = 4;
  internal_max_size_ = 4;
#endif
  // 如果fetch成功，则更新root_page_id_
  /*-----UPDATE-----
   *IndexRootsPage都还没insert过，怎么可能找得到
   *反过来应该是如果成功申请到了rootPage，把它insert进indexRootsPage里
   *但是因为是EmptyTree，root都不需要申请。
  if (root_page->GetRootId(index_id,root_page_id)) {
    root_page_id_ = *root_page_id;
  }*/
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // 如果当前页面 ID 是无效的，则返回
  if (current_page_id == INVALID_PAGE_ID) {
    return;
  }
  // 从缓冲池中获取当前页面
  BPlusTreePage *current_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if (current_page == nullptr) {
    return;
  }
  // 如果当前页面不是叶子页面，则递归销毁其所有子节点,查了一下，说是不用区别对待内部节点和根节点，主要因为根节点的本质就是内部节点
  if (!current_page->IsLeafPage()) {
    BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
    int size = internal_page->GetSize();
    for (int i = 0; i < size; ++i) {
      page_id_t child_page_id = internal_page->ValueAt(i);
      Destroy(child_page_id);
    }
  }
  // 取消固定当前页面并将其从缓冲池中删除
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  buffer_pool_manager_->DeletePage(current_page_id);
}


/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // 如果根节点为空，则树为空，直接返回false
  if (IsEmpty()) {
    return false;
  }
  // 从根节点开始搜索合适的叶子节点
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *current_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());

  page_id_t current_page_id = current_page->GetPageId();
  Page *leaf_page = FindLeafPage(key, current_page_id, transaction);
  if (leaf_page != nullptr) {
    BPlusTreeLeafPage *leaf_node = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
    RowId value;
    if(leaf_node->Lookup(key, value, processor_)) {
      result.push_back(value);
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  // 如果树为空，则创建一个新的根节点
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 申请一个新的页
  page_id_t new_root_page_id;
  /*-----UPDATE-----
   *NewPage不Pin了之后优化了代码结构
   */
  Page *new_root_page = nullptr;
  if ((new_root_page = buffer_pool_manager_->NewPage(new_root_page_id)) == nullptr) {
    LOG(FATAL) << "out of memory";
    return;
  }
  root_page_id_ = new_root_page_id;
  UpdateRootPageId(true); //insert
  // 申请一个新的叶子节点
  page_id_t new_leaf_page_id;
  LeafPage *new_leaf_page = nullptr;
  if ((new_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(new_leaf_page_id)->GetData())) == nullptr) {
    LOG(FATAL) << "out of memory";
  }
  // 对叶子节点的内容进行初始化和插入
  /*-----UPDATE------
   * 还需要加上next_page_id的更新
   */
  new_leaf_page->Init(new_leaf_page_id, new_root_page_id,processor_.GetKeySize(), leaf_max_size_);//----update---- 这里的KeySize传入的应该不是0叭
  new_leaf_page->Insert(key, value, processor_);
  new_leaf_page->SetNextPageId(INVALID_PAGE_ID); // 下一个page是INVALID
  new_leaf_page->SetPrevPageId(INVALID_PAGE_ID); // prev=INVALID
  // -----UPDATE--------------------------**不一定对**-------------------------
  // root 作为一个internal node，应当有个初始化的过程.
  InternalPage *the_root_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
  the_root_page->Init(new_root_page_id,INVALID_PAGE_ID,processor_.GetKeySize(),internal_max_size_);
  the_root_page->SetValueAt(0, new_leaf_page_id); // key = invalid, 没写
  the_root_page->IncreaseSize(1);
  // 固定新节点并将其写入磁盘
  buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // 先找到叶子节点是否存在
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  // 如果叶子节点不存在则退出
  if (leaf_page == nullptr) {
    return false;
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  RowId value_tmp = value;
  // 如果该叶子节点有相同的key，则返回false
  if (leaf_node->Lookup(key, value_tmp, processor_)) {
    return false;
  }
  // 不存在则插入新的键值对
  leaf_node->Insert(key, value, processor_);
  // 如果插入后超出最大值，则分裂，更新父节点的部分已经在split中实现
  if (leaf_node->GetSize() > leaf_node->GetMaxSize()) {
    LeafPage *split_leaf_page = Split(leaf_node, transaction);
    buffer_pool_manager_->UnpinPage(split_leaf_page->GetPageId(), true);
  }
  // 固定节点并将其写入磁盘
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
// 将更新父节点的部分封装进来了，不知道会不会出问题,因为调用了insertintoparent，但是这个函数的实现有可能要调用split，不知道会不会爆，先留个warning!!!!!
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  /*-----UPDATE-----
   *NewPage不Pin了之后优化了代码结构
   */
  Page *new_page = nullptr;
  if ((new_page = buffer_pool_manager_->NewPage(new_page_id)) == nullptr) {
    LOG(FATAL) << "out of memory";
    return nullptr;
  }
  // 转化为新的中间节点，并且将已有内容的一半移动到新的节点
  InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_); // ---bug--- 0->KeySize()
  node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  // 更新父节点
  /*-----UPDATE----
   *意想不到的oop
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
   */
  // 这里的key大概就是middlekey(可以参考internal中的实现）
  // 对的，并且middlekey存放在internalpage的冗余key的位置上，因此默认是无用的，不需要删除处理。
  InsertIntoParent(node,new_internal_node->KeyAt(0),new_internal_node,transaction);
  return new_internal_node;
}
// 树叶的split方法和中间节点的逻辑相同
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  /*-----UPDATE-----
   *NewPage不Pin了之后优化了代码结构
   */
  Page *new_page = nullptr;
  if ((new_page = buffer_pool_manager_->NewPage(new_page_id)) == nullptr) {
    LOG(FATAL) << "out of memory";
    return nullptr;
  }
  // 转化为新的叶子节点，并且将已有内容的一半移动到新的节点
  /*-----UPDATE-----
   *SPLIT 后应该注意next_page_id的变更，用于index遍历
   */
  LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  // 更新newleaf的状态
  new_leaf_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);//---UPDATAE--- 同上
  new_leaf_node->SetNextPageId(node->GetNextPageId());
  new_leaf_node->SetPrevPageId(node->GetPageId());
  //更新nextleaf的状态
  if (node->GetNextPageId()!=INVALID_PAGE_ID) {
    Page *next_page = buffer_pool_manager_->FetchPage(node->GetNextPageId());
    LeafPage *next_leaf = reinterpret_cast<LeafPage *>(next_page->GetData());
    next_leaf->SetPrevPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(next_leaf->GetPageId(),true);
  }
  //更新node的状态
  node->SetNextPageId(new_page_id); // 更新next_page_id

  node->MoveHalfTo(new_leaf_node);
  // 更新父节
  InsertIntoParent(node,new_leaf_node->KeyAt(0),new_leaf_node,transaction);
  //InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));// ----UPDATE----顺序换了一下，因为可能是newpage
  //buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return new_leaf_node; //---UPDATE--- 新页传回Insert函数Unpin
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    // 如果是根节点，则创建一个新的根节点，并且插入新的键值对
    page_id_t new_root_page_id;
    Page *new_root_page = nullptr;
    if ((new_root_page = buffer_pool_manager_->NewPage(new_root_page_id)) == nullptr) {
      LOG(FATAL) << "out of memory";
      return;
    }
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    root_page_id_ = new_root_page_id;
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    UpdateRootPageId(false);
    return;
  }
  // 如果不是根节点，则找到父节点，并且插入新的键值对
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 如果父节点溢出，则进行进一步地split
  if(parent->GetSize() > parent->GetMaxSize()) {
    // 注意此处split内部包含了insertintoparent，已经有一个递归在其中了
    InternalPage *split_page = Split(parent, transaction);
    buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true); //补上了，能对称就对称
    // split中已经unpin了，这里就不调buffer_managerunpin，如果有问题可以补上！！！！！
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  // 如果树为空，则直接返回
  if (IsEmpty()) {
    return;
  }
  // 查找key所在叶节点
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(key, root_page_id_, false)->GetData());
  // 如果叶节点不存在，则退出
  if (leaf_node == nullptr) {
    return;
  }
  // 查找key是否存在
  RowId value_tmp;
  if (!leaf_node->Lookup(key, value_tmp, processor_)) {
    return;
  }

  InternalPage *root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  bool only_child = root->GetSize()==1;
  bool is_first = (processor_.CompareKeys(key, leaf_node->KeyAt(0))==0)&& !(only_child);

  // 删除key对应的value
  int left_page_size = leaf_node->RemoveAndDeleteRecord(key, processor_);
  if (left_page_size == 0 && only_child) { // 最后一个元素也被删除了
    AdjustRoot(root);
  }

  buffer_pool_manager_->UnpinPage(root_page_id_,false);
  // 如果删除节点后，节点的大小小于最小值，则进行合并
  /*-----UPDATE-----
   *如果remove的key在leafPage中的第一个，这里上层的key可能要变
   *when?
   *当：不是第一个的leafpage中，key[0]发生改变时
   *不是第一个如何判断？
   *答：key<Root->KeyAt(1);
   *因为是第一个，所以要么是lookup能找到的，要么就是上一层的最左leafpage
   */
  if (is_first) {
    GenericKey *newKey = leaf_node->KeyAt(0); // 新的Key
    page_id_t parent_page_id = leaf_node->GetParentPageId();
    while (parent_page_id!=INVALID_PAGE_ID) {
      InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));
      if (processor_.CompareKeys(key, parent_page->KeyAt(1))<0) parent_page_id = parent_page->GetParentPageId();
      else { // 就在这一个page可以找到
        page_id_t child_id = parent_page->Lookup(key, processor_);
        parent_page->SetKeyAt(parent_page->ValueIndex(child_id), newKey);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        break;
      }
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
    }
  }

  if(!only_child && left_page_size < leaf_node->GetMinSize()) {
    // 如果成功地进行了合并或者重新分配，则删除原有的节点页
    if(CoalesceOrRedistribute(leaf_node, transaction)) { // 进行了合并
      /*-------UPDATE------
       *为了pin的正确性考虑，在这里加入internalpage向上调整的逻辑
       */
      page_id_t now_page_id = leaf_node->GetParentPageId();
      InternalPage *now_page;
      // 循环检查是否需要对上层的internal page进行调整
      int now_page_size;
      while (now_page_id!=INVALID_PAGE_ID) { //
        now_page = reinterpret_cast<InternalPage *> (buffer_pool_manager_->FetchPage(now_page_id));
        now_page_size = now_page->GetSize();
        if (now_page_size<now_page->GetMinSize()) {
          if (CoalesceOrRedistribute(now_page, transaction)) { //合并了，说明可能要继续向上调整
            now_page_id = now_page->GetParentPageId();
            buffer_pool_manager_->UnpinPage(now_page->GetPageId(), true);
            buffer_pool_manager_->DeletePage(now_page->GetPageId()); //删掉Internal page
          }
          else { buffer_pool_manager_->UnpinPage(now_page_id, true); break;} //没合并，不会再继续向上调整了。
        }
        else { buffer_pool_manager_->UnpinPage(now_page_id, false); break;}
      }
      /*-----------------------*/
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(leaf_node->GetPageId()); //删掉叶子page
    }
    else buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}


/* todo
 *
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
/* -----UPDATE------
 * 缺少向上传递
 */
template <typename N> // internal or leaf
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  /*----UPDATE----
   *AdjustRoot有两种调用情况，判断语句有错误。
   *另一种为当最后一个叶子元素也被删除时
   */
  if(node->IsRootPage()) {
    if (node->GetSize() == 1)
    return AdjustRoot(node);//减少一层
    return false;
  }
  // 先找父节点，再找到兄弟节点
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // -----UPDATE--------如果就一个孩子,即就一个叶子结点
  if (parent->GetSize() == 1) return false;
  page_id_t sibling_page_id = INVALID_PAGE_ID;
  // 找到当前节点在父节点中的索引
  int index = parent->ValueIndex(node->GetPageId());
  if(index == 0)
    sibling_page_id = parent->ValueAt(index + 1);
  else
    sibling_page_id = parent->ValueAt(index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
  // 如果兄弟节点的大小+当前节点的大小>最大值，则进行 Redistribution
  if(sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    return false;
  }
  // 如果兄弟节点的大小+当前节点的大小<=最大值，则进行 Coalesce,并在父节点中删除当前节点的索引
  else {
      Coalesce(sibling, node, parent, index, transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling_page_id, true);
      return true;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // 在上层调用里直接考虑了是根节点或者溢出的逻辑，这里就不作考量了
  node->MoveAllTo(neighbor_node,index);

  if (index) {
    if (node->GetNextPageId()!=INVALID_PAGE_ID) {
      Page *next = buffer_pool_manager_->FetchPage(node->GetNextPageId());
      LeafPage *next_node = reinterpret_cast<LeafPage*>(next->GetData());
      next_node->SetPrevPageId(neighbor_node->GetPageId());
      buffer_pool_manager_->UnpinPage(next_node->GetPageId(),true);
    }
    neighbor_node->SetNextPageId(node->GetNextPageId());
  }
  else {
    if (node->GetPrevPageId()!=INVALID_PAGE_ID) {
      Page *prev =  buffer_pool_manager_->FetchPage(node->GetPrevPageId());
      LeafPage *prev_node = reinterpret_cast<LeafPage*>(prev->GetData());
      prev_node->SetNextPageId(neighbor_node->GetPageId());
      buffer_pool_manager_->UnpinPage(prev_node->GetPageId(), true);
    }
    neighbor_node->SetPrevPageId(node->GetPrevPageId());
  }


  /*------UPDATE------
   * BUG - 这里node可能是index = 0的点，而MoveAllTo的逻辑是 index != 0 的点的逻辑 (Completed)
   */
  // 根据上一个函数规定的次序，如果nodeindex=0则node在最左边，它的nextpageid就是sibling的id，不做处理,否则sibling在node的左边，需要继承node的nextpageid
  parent->Remove(index); // index 是当前node在parent中的index

  /*---------UPDATE--------
   *1. 当remove后上层节点的key个数小于minSize时，向上传递remove操作, 同时需要删除internalpage (completed)
   *2. 如果删了leaf_page 的第一个key， 上层的key也要变 (--------------------------)
   */
  return true;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  GenericKey *middle_key;
  // middle_key就是父节点的中间索引，如果index=0，则middle_key就是父节点的第一个索引，否则middle_key就是sibling对应的索引
  if(index != 0)
    middle_key = parent->KeyAt(index);
  else
    middle_key = parent->KeyAt(1);
  /*------UPDATE------
   * BUG - 这里node可能是index = 0的点，而MoveAllTo的逻辑是 index != 0 的点的逻辑 (COMPLETED)
   */
  node->MoveAllTo(neighbor_node,middle_key,buffer_pool_manager_,index);
  parent->Remove(index);
  return true;
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
// 这里的逻辑比我想的寡一点，我还以为要split，虽然脑测一下感觉也是对的（一个个挪）,但我感觉不太踏实www
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  /*----UPDATE-----
   *即使是在leaf层的redistribute，也是会改变parent中的key的
   *因此这里还需要对parent对应index的key进行set
   */
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if(index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  }
  else {
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index,node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  GenericKey *middle_key;
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if(index == 0) {
    middle_key = parent->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node,middle_key,buffer_pool_manager_);
  }
  else {
    middle_key = parent->KeyAt(index - 1);
    neighbor_node->MoveLastToFrontOf(node,middle_key,buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  InternalPage * root= reinterpret_cast<InternalPage *>(old_root_node);
  if(old_root_node->GetSize() == 1) {
    // 这里不仅和上层调用函数 coalesceOrRedistribute() 相关（仅在是root的情况调用），还和新树插入子节点的逻辑相关，我印象里当时实现root节点不会作为叶子节点插入的，所有此处不考虑root为叶子节点的情况
    // case 1: delete the last element in root page, but root page still has one last child
    auto *root = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = root->RemoveAndReturnOnlyChild();
    Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    this->root_page_id_ = new_root_page_id;
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    UpdateRootPageId(false);
    return true;
  }
  else {
    // case 2: delete the last element in whole b+ tree
    root_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->DeletePage(root->ValueAt(0));
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    UpdateRootPageId(false); // ---update----
    return true;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {

  if (IsEmpty()) {
    return IndexIterator();
  }
  // 找到最左边的叶子节点
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(nullptr, root_page_id_, true)->GetData());
  // 构造index iterator
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, 0); //-----UPDATE----- index para From size to 0
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(key, root_page_id_, false)->GetData());
  int index = leaf_node->KeyIndex(key, processor_);
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, index); //-----UPDATE-----
}


/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  /*LeafPage *leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(nullptr, root_page_id_, true)->GetData());
  while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf_node->GetNextPageId();
    buffer_pool_manager_->FetchPage(next_page_id);
    leaf_node = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, leaf_node->GetSize()-1); // --bug-- 小bug size-1是最后一个
  */
  /* -----
   * 目前END是最后一个元素，但实际使用时一般是最后一个的后一个（INVALID），到时候可能还要改动
   * ------
   * 坏消息，看了一下test，真的要改
   */
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}



/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key , page_id_t page_id, bool leftMost) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while(b_plus_tree_page->IsLeafPage() == false) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    page_id_t next_page_id = INVALID_PAGE_ID;
    if(leftMost || internal_page->GetSize()==1) // ----bug---- 当为只有1个child 的根节点时也直接选ValueAt（0）
      next_page_id = internal_page->ValueAt(0);
    else {
      next_page_id = internal_page->Lookup(key, processor_);
    }
    ASSERT(next_page_id!=INVALID_PAGE_ID,"Invalid next PageID in ::FindLeafPage");
    buffer_pool_manager_->UnpinPage(b_plus_tree_page->GetPageId(), false);
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  // ---------UPDATE-------
  // Unpin的逻辑完善。
  return reinterpret_cast<Page *>(b_plus_tree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
// 这里对于输入参数进行了修改，原来的输入是int,但我感觉boolg更合适
void BPlusTree::UpdateRootPageId(bool insert_record) {
  auto *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto *Index_root_page = reinterpret_cast<IndexRootsPage *>(page->GetData());

  if(insert_record){
    Index_root_page->Insert(index_id_, root_page_id_);
  }
  else{
    Index_root_page->Update(index_id_, root_page_id_);
  }

  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  //buffer_pool_manager_->UnpinPage(root_page_id_, true);-----bug---- 不用unpin，这个函数里面没有Fetch过
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}