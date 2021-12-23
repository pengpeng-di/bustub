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
#include "include/common/logger.h"
namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    capacity = num_pages;
    clock_handle_pos = 0;
    size = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { //只有这个方法会改变clock_hand的值,这时应该size == capacity
    LOG_INFO("%d", *frame_id);
    clock_lacth.WLock();
    for(size_t i=clock_handle_pos; i<size; i++){
        if(clock_container[i].ref == 0){
            frame_id = const_cast<frame_id_t*>(&(clock_container[i].frame_id));
            clock_handle_pos ++;
            return true;
        }
        clock_handle_pos = (clock_handle_pos+1)%size;
        clock_container[i].ref = 0;
    }
    clock_lacth.WUnlock();
    return true; 
}          

void ClockReplacer::Pin(frame_id_t frame_id) {
    for(size_t i=0;i<size;i++){
        if(clock_container[i].frame_id ==frame_id){
            clock_container[i].ref = 1;
        }
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    for(size_t i=0;i<size; i++){
        if(clock_container[i].frame_id == frame_id){
            clock_container[i].ref = 0;
            return ;
        }
    }

    clock_container.emplace_back(node{frame_id, 1});
    size ++;
}

size_t ClockReplacer::Size() {
    int count = 0;
    for(size_t i=0;i<size; i++){
        if(clock_container[i].ref == 0){
            count ++;
        }
    }
    return count;
}

}  // namespace bustub
