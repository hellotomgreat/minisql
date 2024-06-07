#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"
#define test
static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
//  LOG(INFO)<<"--INTO: FetchPage--"<<std::endl;
  if (page_id == INVALID_PAGE_ID) return nullptr;
  // 1.     Search the page table for the requested page (P).
  frame_id_t frame_id;
  Page *P;
  unordered_map<page_id_t, frame_id_t>::iterator P2F;
  if (page_table_.count(page_id) != 0) { // Check if page exists
    P2F = page_table_.find(page_id);
    frame_id = P2F->second;
    P = &pages_[frame_id];
    // 1.1    If P exists, pin it and return it immediately.
    replacer_->Pin(frame_id);
    P->pin_count_++;
    //LOG(INFO)<< "--IN--FetchPage: PageInPool return"<<std::endl;
    return P;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //         Note that pages are always found from the free list first.
  Page *R = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    R = &pages_[frame_id];
  }
  else {
    frame_id_t victim_id;
    if(replacer_->Victim(&victim_id)) {
      R = &pages_[victim_id];
    }
    else {
      //LOG(INFO)<< "--IN--FetchPage: NoPoolSpace return"<<std::endl;
      return nullptr;
    }
  }
  // 2.     If R is dirty, write it back to the disk.
  if(R->IsDirty()) {
    disk_manager_->WritePage(R->page_id_, R->data_);
    /*
    *---UPDATE--- 之前没有更新dirty，但实际没细想有没有大问题
    */
    R->is_dirty_ = false;
  }
  // 3.     *****Delete R from the page table and insert P.这里我不知道P的信息哪里来的，主要是困惑是否需要将R的pageid,frameid继承到P中
  // 应当从disk中找到P---若disk中也没有？应当返回INVALID_PAGEID?
  frame_id_t R_frame_id = page_table_.find(R->page_id_)->second;
  page_table_.erase(R->page_id_);
  P = R;
  P->page_id_ = page_id;
  page_table_.insert(std::make_pair(P->page_id_, R_frame_id));
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, P->GetData());
  //LOG(INFO) << "In **FetchPage**: Successfully Get Page With Data:"<<P->GetData()<<std::endl;
  P->is_dirty_ = false;
  P->pin_count_ = 1;
  return P;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  Page *P;
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  size_t i;
  frame_id_t frame_id;
  for ( i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) { // ==0才退出
      break;
    }
  }
  if(i == pool_size_) {
    page_id = 0;
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    P = &pages_[frame_id];
  }
  else {
    if(replacer_->Victim(&frame_id)) {
      P = &pages_[frame_id];
      /*
       *---UPDATE--- New过程也会replace一些page，这里也需要进行处理（上面Fetch的处理应该是同理可以放到else里面的）
       */
      if(P->IsDirty()) {
        disk_manager_->WritePage(P->page_id_, P->data_);
        P->is_dirty_ = false;
      }
      page_table_.erase(P->GetPageId());//如果是替换的，需要把原来的page从table里移除
    }
    else {
      page_id = 0;
      return nullptr;
    }
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  P->ResetMemory();
  P->page_id_ = AllocatePage();
  page_table_.insert(std::make_pair(P->page_id_, frame_id));
  if(P->is_dirty_) {
    disk_manager_->WritePage(P->page_id_, P->data_);
    P->is_dirty_ = false;
  }
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = P->GetPageId();
  return P;

}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   If P does not exist, return true.
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  Page *P = &pages_[page_table_.find(page_id)->second];
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(P->pin_count_ != 0 ) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  frame_id_t frame_id = page_table_.find(page_id)->second;
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  /*
   *---UPDATE--- 不加上这句，则没有实际上在disk_manager中释放page
   */
  DeallocatePage(P->GetPageId());
  P->ResetMemory();
  P->page_id_ = INVALID_PAGE_ID;
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.count(page_id) == 0) {
    return false;
  }

  Page *P = &pages_[page_table_.find(page_id)->second];
  if(P->pin_count_ == 0) {
    return true;
  }

  if(is_dirty) {
    P->is_dirty_ = true;
  }

  P->pin_count_--;
  if (P->pin_count_ == 0) replacer_->Unpin(page_table_.find(page_id)->second);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  if(page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  Page *P = &pages_[page_table_.find(page_id)->second];
  disk_manager_->WritePage(page_id, P->GetData());
  LOG(INFO) <<"In FlushPage: Successfully Write in--"<<P->GetData()<<std::endl;
  P->is_dirty_ = false;
  latch_.unlock();
  return true;

}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}