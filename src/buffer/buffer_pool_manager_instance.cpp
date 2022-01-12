//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/macros.h"
#include "include/common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  std::lock_guard<std::mutex> lck(latch_);
  Page *tmp;
  for (size_t i = 0; i < (pool_size_ - free_list_.size()); i++) {  // the number buffer_pool already use
    if (pages_[i].GetPageId() == page_id) {
      tmp = pages_ + i;
      if (nullptr == tmp || tmp->page_id_ == INVALID_PAGE_ID) {
        return false;
      }
      if (pages_[i].is_dirty_) {
        disk_manager_->WritePage(page_id, pages_[i].data_);
        pages_[i].is_dirty_ = false;
      }
    }
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lck(latch_);
  Page *tmp;
  for (size_t i = 0; i < (pool_size_ - free_list_.size()); i++) {
    tmp = pages_ + i;
    if (nullptr == tmp || tmp->page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    if (pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t tmp_frame_id;
  Page *tmp_page;
  page_id_t tmp_page_id;
  if (free_list_.empty()) {
    if (replacer_->Size() != 0) {
      replacer_->Victim(&tmp_frame_id);
    } else {
      return nullptr;
    }
  } else {
    tmp_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  tmp_page = pages_ + tmp_frame_id;
  if (tmp_page->pin_count_ != 0) {
    return nullptr;
  }

  tmp_page_id = AllocatePage();
  *page_id = tmp_page_id;

  if (tmp_page->is_dirty_) {
    disk_manager_->WritePage(tmp_page->GetPageId(), tmp_page->data_);
  }

  // update page_table
  std::unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(tmp_page_id);
  if (iter != page_table_.end()) {
    page_table_.erase(iter);
  }
  page_table_.insert(std::pair<page_id_t, frame_id_t>(tmp_page_id, tmp_frame_id));

  // reset metadata
  tmp_page->ResetMemory();
  tmp_page->page_id_ = tmp_page_id;
  tmp_page->is_dirty_ = false;
  tmp_page->pin_count_ = 1;

  return tmp_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lck(latch_);
  frame_id_t tmp_frame_id;
  Page *tmp_page;
  // 1.1
  std::unordered_map<page_id_t, frame_id_t>::iterator iter;

  iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    tmp_page = pages_ + iter->second;
    tmp_page->pin_count_ += 1;
    replacer_->Pin(iter->second);
    return tmp_page;
  }
  if (free_list_.empty()) {
    return nullptr;
  }

  // 1.2
  if (free_list_.empty()) {
    replacer_->Victim(&tmp_frame_id);
  } else {
    tmp_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  tmp_page = pages_ + tmp_frame_id;

  // 2
  if (tmp_page->is_dirty_) {
    disk_manager_->WritePage(tmp_page->GetPageId(), tmp_page->data_);
  }

  // 3
  iter = page_table_.find(tmp_frame_id);
  if (iter != page_table_.end()) {
    page_table_.erase(iter);
  }
  page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, tmp_frame_id));

  // 4
  tmp_page->ResetMemory();
  tmp_page->page_id_ = page_id;
  tmp_page->is_dirty_ = false;
  tmp_page->pin_count_ = 1;
  disk_manager_->ReadPage(tmp_page->page_id_, tmp_page->data_);  // we should read data from disk back
  return tmp_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lck(latch_);
  std::unordered_map<page_id_t, frame_id_t>::iterator iter;
  Page *tmp_page;
  // 1
  iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  tmp_page = pages_ + iter->second;

  // 2
  if (tmp_page->pin_count_ > 0) {
    return false;
  }

  // 3
  replacer_->Unpin(iter->second);
  tmp_page->ResetMemory();
  tmp_page->is_dirty_ = false;
  page_table_.erase(iter);
  free_list_.emplace_back(iter->second);

  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lck(latch_);
  std::unordered_map<page_id_t, frame_id_t>::iterator iter;
  frame_id_t tmp_frame_id = 0;
  Page *tmp_page;

  for (iter = page_table_.begin(); iter != page_table_.end(); iter++) {
    if (iter->first == page_id) {
      tmp_frame_id = iter->second;
      break;
    }
  }

  tmp_page = pages_ + tmp_frame_id;
  tmp_page->pin_count_--;

  if (tmp_page->pin_count_ > 0) {
    return false;
  }
  if (is_dirty) {
    disk_manager_->WritePage(page_id, tmp_page->data_);
  }

  replacer_->Unpin(tmp_frame_id);
  free_list_.emplace_back(tmp_frame_id);
  if (iter == page_table_.end()) {
    return false;
  }
  page_table_.erase(iter);
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
