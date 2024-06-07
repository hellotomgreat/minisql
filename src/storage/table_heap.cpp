#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

// debug warings !! 这一部分代码可能和前文实现的内容关系非常紧密，而且我倾向于直接调用封装好的内容
// 如果出现不期望的结果，建议查看row部分的实现（因为row的实现感觉很灵活，有可能出错）
// 关于bonus,感觉可以直接引入一个nextpageid以及full_or_not来避免在已有链表中的遍历
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  LOG(INFO) << "---Into TableHeap::InsertTuple---" << std::endl;
  if (row.GetSerializedSize(this->schema_) > PAGE_SIZE){
    return false;
  }
  page_id_t first_page_id = this->GetFirstPageId();
  auto *first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id));
  TablePage *now_page = first_page;
  page_id_t now_page_id = now_page->GetTablePageId();
  // 如果一直插入失败则一直查询，直到找到可以插入的一页
  while(!(now_page->InsertTuple(row,schema_, txn, this->lock_manager_, this->log_manager_))) {
    now_page_id = now_page->GetPageId();
    // 如果还有下一页（还没到链表的尽头）
    if(now_page->GetNextPageId() != INVALID_PAGE_ID) {
      page_id_t next_page_id = now_page->GetNextPageId();
      // 跳转到下一页
      /*
       *---UPDATE----
       *每一次Fetch完的page不要用了都应当Unpin
       */
      buffer_pool_manager_->UnpinPage(now_page_id,false); //因为是没能成功插入的page，没有修改数据。
      now_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
    else{
      // 如果已经到链表的尽头，则创建一个新的页，并插入
      page_id_t next_page_id;
      if(buffer_pool_manager_->NewPage(next_page_id)) {
        // 跳转到新页并连接到原有链表
        buffer_pool_manager_->UnpinPage(now_page_id,false); //因为是没能成功插入的page，没有修改数据。
        now_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
        now_page->Init(next_page_id,now_page_id,this->log_manager_,txn);
        //now_page->SetNextPageId(INVALID_PAGE_ID);
        //now_page->SetPrevPageId(now_page_id); ---UPDATE----INIT里面已经进行了同样操作
      }
    }
  }
  buffer_pool_manager_->UnpinPage(now_page_id,true); //这里是成功插入的page，dirty了
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {

  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { //rid??----------------------------------debug
  // 我在这一部分修改了table_page中update的返回值用于判断更新失败的原因并尝试解决由于空间不足导致的问题，英文注释部分就是table_page.cpp中注释
  LOG(INFO) << "---Into TableHeap::UpdateTuple---" << std::endl;
  if (row.GetSerializedSize(this->schema_) > PAGE_SIZE) {
    return false;
  }
  // 找到原有row所在的page
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->WLatch();
  Row *old_row = new Row(rid);
  int judge_flag =page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_);
  switch (judge_flag) {
    case 1:
      // update success
        page->WUnlatch();
    return true;
    case -1:
      // If the slot number is invalid, abort.
        page->WUnlatch();
        LOG(INFO)<< "Invalid slot number"<<std::endl;
    return false;
    case -2:
      // If the tuple is deleted, abort.
        page->WUnlatch();
        LOG(INFO)<< "Tuple is deleted"<<std::endl;
        return false;
    case -3:
      // If there is not enough space to update, we need to update via delete followed by an insert (not enough space).
        // 这里的颗粒度我不确定，不过我考虑的是如果page层面无法解决空间分配的问题，索性直接在heap层面去解决，即先删除再插入，如果删除失败则直接返回false，如果插入成功则返回true，如果插入失败则返回false
          if(this->MarkDelete(rid, txn) && this->InsertTuple(row,txn)) {
            page->WUnlatch();
            return true;
          }
          else {
            page->WUnlatch();
            LOG(INFO)<< "Update failed "<<std::endl;
            return false;
          }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
}
/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // 这个函数好像和markdelete在结构上对称欸
  // Step1: Find the page which contains the tuple.
  LOG(INFO) << "---Into TableHeap::ApplyDelete---" << std::endl;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==nullptr){
    return;
  }
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  LOG(INFO) << "---Into TableHeap::RollbackDelete---" << std::endl;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  LOG(INFO) << "---Into TableHeap::GetTuple---" << std::endl;
  RowId rid = row->GetRowId();
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->RLatch();
  // 我参考的代码部分需要在这里unpinpage，不过我不是很理解，就没加
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(),true);
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id) { //删整个堆表
  LOG(INFO) << "---Into TableHeap::DeleteTable---" << std::endl;
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  // 根据框架样例，这里应该返回的是row的容器
  LOG(INFO) << "---Into TableHeap::Begin---" << std::endl;
  first_page_id_ = this->GetFirstPageId();
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  auto page_now = first_page;
  RowId *first_rid;
  while (page_now != nullptr) {
    page_now->RLatch();
    if (page_now->GetFirstTupleRid(first_rid)) { //防止是空页
      page_now->RUnlatch();
      break;
    }
    page_now->RUnlatch();
    page_now = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_now->GetNextPageId()));
  }
  buffer_pool_manager_->UnpinPage(page_now->GetPageId(), false);
  return TableIterator(this, RowId(*first_rid), txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  LOG(INFO) << "---Into TableHeap::End---" << std::endl;
  return TableIterator(this, INVALID_ROWID, nullptr);
}