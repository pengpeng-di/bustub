//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t global_depth_mask = dir_page->GetGlobalDepthMask();
  uint32_t hash_key = Hash(key);
  return hash_key & global_depth_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *result;
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // 创建一个HashTableDirectoryPage
    page_id_t new_page_id_dir;
    Page *page = buffer_pool_manager_->NewPage(&new_page_id_dir);
    assert(page != nullptr);

    // HashTableDirectoryPage的内容是Page中的data_部分的内容
    result = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    directory_page_id_ = new_page_id_dir;
    result->SetPageId(directory_page_id_);

    // 然后创建一个bucket
    page_id_t new_page_id_buc;
    page = nullptr;
    page = buffer_pool_manager_->NewPage(&new_page_id_buc);
    assert(page != nullptr);
    result->SetBucketPageId(0, new_page_id_buc);

    // unpin这两个page
    assert(buffer_pool_manager_->UnpinPage(new_page_id_dir, true));
    assert(buffer_pool_manager_->UnpinPage(new_page_id_buc, true));
  }

  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  result = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());

  // 1、bucket_page_id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);

  // 2、bucket_page
  HASH_TABLE_BUCKET_TYPE *buc_type = FetchBucketPage(bucket_page_id);
  buc_type->GetValue(key, comparator_, result);

  // 3、unpin相应的page
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1、bucket_page_id
  // Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  // assert(page != nullptr);
  // HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);

  // 2、bucket_page
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(bucket_page_id);
  assert(buc_page != nullptr);

  // 3、如果bucket还没满
  if (!buc_page->IsFull()) {
    bool ret = buc_page->Insert(key, value, comparator_);
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    return ret;
  }
  // 4、如果bucket满了
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1、获取bucket_idx
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t split_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_idx);
  uint32_t split_bucket_page_id = dir_page->GetBucketPageId(split_bucket_idx);
  HASH_TABLE_BUCKET_TYPE *split_buc_page = FetchBucketPage(dir_page->GetBucketPageId(split_bucket_idx));

  // 2、判断directory是否需要扩容
  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 3、增加local depth
  dir_page->IncrLocalDepth(split_bucket_idx);

  // 4、获取当前需要扩容的bucket，将里面的数据库全部保存下来
  MappingType *split_bucket_data = split_buc_page->GetAllData();
  uint32_t old_bucket_size = split_buc_page->NumReadable();
  split_buc_page->ClearBucket();

  // 5、分配image_bucket
  page_id_t image_bucket_page_id;
  Page *page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
  assert(page != nullptr);
  HASH_TABLE_BUCKET_TYPE *image_buc_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  uint32_t image_bucket_idx = dir_page->GetSplitImageIndex(split_bucket_idx);
  dir_page->SetBucketPageId(image_bucket_idx, image_bucket_page_id);
  dir_page->SetLocalDepth(image_bucket_idx, dir_page->GetLocalDepth(split_bucket_idx));

  // 6、重新插入数据
  for (uint32_t i = 0; i < old_bucket_size; i++) {
    MappingType tmp = split_bucket_data[i];
    uint32_t target_bucket_idx = Hash(tmp.first) & dir_page->GetLocalDepthMask(split_bucket_idx);
    page_id_t target_page_id = dir_page->GetBucketPageId(target_bucket_idx);
    assert(target_page_id == split_bucket_page_id || target_page_id == image_bucket_page_id);
    if (target_page_id == split_bucket_page_id) {
      split_buc_page->Insert(key, value, comparator_);
    } else {
      image_buc_page->Insert(key, value, comparator_);
    }
  }
  delete[] split_bucket_data;

  // 7、将同一级的bucket设置为相同的depth和page
  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_idx);
  for (uint32_t i = split_bucket_idx; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
  }

  for (uint32_t i = split_bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
    dir_page->SetBucketPageId(i, split_bucket_page_id);
  }

  for (uint32_t i = image_bucket_idx; i >= diff; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
    dir_page->SetBucketPageId(i, image_bucket_page_id);
  }

  for (uint32_t i = image_bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_idx));
    dir_page->SetBucketPageId(i, image_bucket_page_id);
  }

  // 8、unpin用到的页面
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page_id, true));

  // 9、重新插入数据
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(bucket_page_id);

  bool ret = buc_page->Remove(key, value, comparator_);
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));

  if (buc_page->IsEmpty()) {
    Merge(transaction, key, value);
  }

  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 1、get metadata
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *target_buc_page = FetchBucketPage(buc_page_id);
  uint32_t target_bucket_idx = KeyToDirectoryIndex(key, dir_page);  // 要删除的bucket index
  uint32_t image_bucket_idx = dir_page->GetSplitImageIndex(target_bucket_idx);
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_idx);

  // 2 判断local_depth是不是0
  uint32_t target_local_depth = dir_page->GetLocalDepth(target_bucket_idx);
  if (target_local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // 3
  if (dir_page->GetLocalDepth(target_bucket_idx) != dir_page->GetLocalDepth(image_bucket_idx)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // 4、如果target bucket不为空也不能收缩
  if (!target_buc_page->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // 删除空的page
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
  assert(buffer_pool_manager_->DeletePage(buc_page_id));

  // 设置target bucket的page为image bucket的page
  dir_page->SetBucketPageId(target_bucket_idx, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_idx);
  dir_page->DecrLocalDepth(image_bucket_idx);
  assert(dir_page->GetLocalDepth(target_bucket_idx) == dir_page->GetLocalDepth(image_bucket_idx));

  // 遍历整个directory，将所有指向target bucket page 的 bucket全部重新指向 image bucket page
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == buc_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_idx));
    }
  }

  // shrink directory
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
