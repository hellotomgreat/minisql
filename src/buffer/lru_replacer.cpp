#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.empty()) {
    return false;
  }
  frame_id_t victim_id = lru_list.back();
  lru_list.pop_back();
  *frame_id = victim_id;
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_list.remove(frame_id);
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  for (list<frame_id_t>::iterator it = lru_list.begin(); it!=lru_list.end(); ++it) if (*it == frame_id) return;
  lru_list.push_front(frame_id);
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}