#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn){
  LOG(INFO) << "---INTO: TableIterator constructor---";
  pointer_to_row_ = new Row(rid);
  if(rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap->GetTuple(pointer_to_row_,txn); //获取对应的tuple存入pointer所在位置。
  }
  table_heap_ = table_heap;
  txn_ = txn;
}
TableIterator::TableIterator(const TableIterator &other) {
  LOG(INFO) << "---INTO: TableIterator copy constructor---";
  pointer_to_row_ = new Row(*(other.pointer_to_row_)); //调用row的copy c‘tor
  table_heap_ = other.table_heap_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  LOG(INFO) << "---INTO: TableIterator destructor---";
  delete pointer_to_row_;
};

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->pointer_to_row_->GetRowId() == itr.pointer_to_row_->GetRowId(); // Rowid.h里面定义了RowId::==
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return *pointer_to_row_;
}

Row *TableIterator::operator->() {
  return pointer_to_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  LOG(INFO) << "---INTO: TableIterator copy constructor---";
  pointer_to_row_ = new Row(*(itr.pointer_to_row_)); //调用row的copy c‘tor
  table_heap_ = itr.table_heap_;
  txn_ = itr.txn_;
}

// ++iter
TableIterator &TableIterator::operator++() {
  LOG(INFO) << "---INTO: TableIterator::operator ++it ---";
  RowId rid = pointer_to_row_->GetRowId();
  TablePage *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  RowId next_rid;
  page->GetNextTupleRid(rid, &next_rid);
  while (next_rid.GetPageId()==INVALID_PAGE_ID) { //说明这一tablepage的tuple都遍历完了，尝试fetch下一页。(因为有可能下一页都被deletemark了，所以while）
    page_id_t next_page_id =page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // 下一页存在
      rid.Set(next_page_id, 0);
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
      page->GetNextTupleRid(rid,&next_rid); //目标是拿到next_rid
    }
    else { //没有下一页，即到达end
      pointer_to_row_->SetRowId(INVALID_ROWID); //表示END
      return *this;
    }
  }
  //到这步说明当前的page是非空的，next_rid即下一个tuple的位置
  pointer_to_row_->SetRowId(next_rid);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  LOG(INFO) << "---INTO: TableIterator::operator it++ ---";
  RowId rid = pointer_to_row_->GetRowId();
  TablePage *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  RowId next_rid;
  page->GetNextTupleRid(rid, &next_rid);
  while (next_rid.GetPageId()==INVALID_PAGE_ID) { //说明这一tablepage的tuple都遍历完了，尝试fetch下一页。(因为有可能下一页都被deletemark了，所以while）
    page_id_t next_page_id =page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // 下一页存在
      rid.Set(next_page_id, 0);
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
      page->GetNextTupleRid(rid,&next_rid); //目标是拿到next_rid
    }
    else { //没有下一页，即到达end
      pointer_to_row_->SetRowId(INVALID_ROWID); //表示END
      return TableIterator(table_heap_, next_rid, txn_);
    }
  }
  //到这步说明当前的page是非空的，next_rid即下一个tuple的位置
  pointer_to_row_->SetRowId(next_rid);
  return TableIterator(table_heap_, next_rid, txn_);
}
