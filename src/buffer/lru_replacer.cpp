//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <map>
#include <unordered_map>
#include "common/logger.h"
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    capacity = num_pages;
    size = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    frame_id_t tmp_frame_id = 0;
    replacer_latch.lock();
    if(container.size() == 0){
        return false;
    }

    //不知道怎么回事unordered_map中的值得顺序都和插入的顺序相反,要取第一个元素得遍历到最后
    std::unordered_map<frame_id_t, size_t>::iterator iter = container.begin();
    for(int i=1; i<Size(); i++){
        iter ++;
    }

    //frame_id = const_cast<frame_id_t *>(&(iter->first));            //这行代码取到的值都是0
    *frame_id = iter->first;
    container.erase(iter);
    replacer_latch.unlock();
    return true; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::unordered_map<frame_id_t, size_t>::iterator iter = container.begin();
    replacer_latch.lock();
    for(; iter!=container.end(); iter++){
        if(iter->first == frame_id){
            container.erase(iter);
            break;
        }
    }
    replacer_latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    replacer_latch.lock();
    bool find = false;
    std::unordered_map<frame_id_t, size_t>::iterator iter = container.begin();
    for(; iter!=container.end(); iter++){
        if(iter->first == frame_id){
            find = true;
            break;
        }
    }

    if(!find){
        container.insert(std::pair<frame_id_t, size_t>(frame_id, 1));
    }
    replacer_latch.unlock();
    //std::cout << "lru 的大小为" << Size() << std::endl;
}

size_t LRUReplacer::Size() {
    //return 6;
    return container.size(); 
}

}  // namespace bustub
