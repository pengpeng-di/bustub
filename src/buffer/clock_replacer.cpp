//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    capacity = num_pages;
    clock_handle_pos = 0;
    size = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { //只有这个方法会改变clock_hand的值,这时应该size == capacity
    clock_lacth.WLock();
    for(int i=0; i<capacity; i++){
        if(clock_container[i].ref == 0){
            frame_id = const_cast<frame_id_t*>(&(clock_container[i].frame_id));
            return true;
        }
        clock_handle_pos = (clock_handle_pos+1)%capacity;
        clock_container[i].ref = 0;
    }
    clock_lacth.WUnlock();
    return true; 
}          

void ClockReplacer::Pin(frame_id_t frame_id) {
    for(int i=0;i<size;i++){
        if(clock_container[i].frame_id ==frame_id){
            clock_container.erase(clock_container.begin()+i);
            size -- ;
        }
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    for(int i=0;i<size; i++){
        if(clock_container[i].frame_id == frame_id){
            return ;
        }
    }

    clock_container.emplace_back(new node{frame_id, 1});
    size ++;
}

size_t ClockReplacer::Size() {
    int count = 0;
    for(int i=0;i<size; i++){
        if(clock_container[i].ref == 0){
            count ++;
        }
    }
    return count;
}

}  // namespace bustub
