//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"


namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool is_find = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      is_find = true;
    }
  }
  return is_find;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t pos = -1;
  for (int i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
        return false;  // same key but different value is fine. same key and same value cannot work;
      }
    } else if (pos == -1) {
      pos = i;
      break;
    }
  }

  if (pos == -1) {
    return false;
  }
  array_[pos] = MappingType(key, value);
  SetOccupied(pos);
  SetReadable(pos);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && array_[i].second == value) {
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {  // 将readable_置0即可
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  c &= (~(1 << (bucket_idx % 8)));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  return (c & (1 << bucket_idx % 8)) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {  // 将对应的bit位置1
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  c |= (1 << bucket_idx % 8);
  occupied_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  return (c & (1 << bucket_idx % 8)) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  c |= (1 << bucket_idx % 8);
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint8_t a = 255;  // 11111111
  for (int i = 0; i < BUCKET_ARRAY_SIZE / 8; i++) {
    uint8_t tmp = static_cast<uint8_t>(occupied_[i]);
    if (static_cast<int>(a & tmp) != 255) {
      return false;
    }
  }

  if (BUCKET_ARRAY_SIZE / 8) {  // 存在余数
    uint8_t c = static_cast<uint8_t>(occupied_[BUCKET_ARRAY_SIZE / 8]);
    for (int i = 0; i < BUCKET_ARRAY_SIZE % 8; i++) {
      if ((c & 1) != 1) {
        return false;
      }
      c >>= 1;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  int full_num = BUCKET_ARRAY_SIZE / 8;
  int remain_num = BUCKET_ARRAY_SIZE % 8;
  int count = 0;
  for (int i = 0; i < full_num; i++) {
    uint8_t tmp = static_cast<uint8_t>(readable_[i]);
    for (int j = 0; j < 8; j++) {
      if ((tmp & 1) == 1) {
        count++;
      }
      tmp >>= 1;
    }
  }

  if (remain_num != 0) {
    uint8_t tmp = static_cast<uint8_t>(readable_[full_num]);
    for (int i = 0; i < remain_num; i++) {
      if ((tmp & 1) == 1) {
        count++;
      }
      tmp >>= 1;
    }
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint8_t mask = 255;
  int32_t num = BUCKET_ARRAY_SIZE / 8;
  for (int i = 0; i < num; i++) {
    uint8_t tmp = static_cast<uint8_t>(occupied_[i]);
    if (static_cast<int>(tmp & mask) != 0) {
      return false;
    }
  }

  if (BUCKET_ARRAY_SIZE % 8 != 0) {
    uint8_t tmp = static_cast<uint8_t>(occupied_[num]);
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if (static_cast<int>(tmp & 1) != 0) {
        return false;
      }
      tmp >>= 1;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetAllData() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  int index = 0;
  for (int i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = std::pair<KeyType, ValueType>(KeyAt(i), ValueAt(i));
    }
  }

  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ClearBucket() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
