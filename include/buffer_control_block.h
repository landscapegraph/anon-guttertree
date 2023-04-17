#pragma once
#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>
#include "types.h"

typedef uint32_t buffer_id_t;
typedef uint64_t File_Pointer;

class GutterTree;

/**
 * Buffer metadata class. Care should be taken to synchronize access to the
 * *entire* data structure.
 */
class BufferControlBlock {
private:
  buffer_id_t id;

  // condition variable to determine if the buffer needs to be flushed
  // std::condition_variable needs_flushing;

  // how many items are currently in the buffer
  File_Pointer storage_ptr;

  // where in the file is our data stored
  File_Pointer file_offset;

  /*
   * Check if this buffer needs a flush or if the current write will overflow
   * @param size            the size of the current write
   * @param flush_size      if the buffer will be at least this full then we need to flush
   * @param max_size        if the buffer will be at least this full that would cause overflow
   * @return true           if buffer needs a flush, false if not
   * @throw BufferFullError if the write will overflow the buffer size.
   */
  bool check_size_limit(uint32_t size, uint32_t flush_size, uint32_t max_size);

  // lock for controlling read/write access to the buffer
  std::mutex RW_lock;

  // lock for controlling flushing this block and its sub-tree
  std::mutex flush_lock;

public:
  // this node's level in the tree. 0 is root, 1 is it's children, etc
  uint8_t level;

  // the index in the buffers array of this buffer's smallest child
  buffer_id_t first_child = 0;
  uint16_t children_num = 0;     // and the number of children

  // information about what keys this node will store
  node_id_t min_key;
  node_id_t max_key;

  /**
   * Generates metadata and file handle for a new buffer.
   * @param id an integer identifier for the buffer.
   * @param off the offset into the file at which this buffer's data begins
   * @param level the level in the tree this buffer resides at
   */
  BufferControlBlock(buffer_id_t id, File_Pointer off, uint8_t level);

  /*
   * Write to the buffer managed by this metadata.
   * @param the buffer tree this control block is a part of
   * @param data the data to write
   * @param size the size in bytes of the data to write
   * @return true if buffer needs flush and false otherwise
   */
  bool write(GutterTree *bf, char *data, uint32_t size);

  // synchronization functions. Should be called when root buffers are read or written to.
  // Other buffers should not require synchronization
  // We keep two locks to allow writes to root to happen while flushing
  // but need to enforce that flushes are done sequentially
  void inline lock_flush()   {if (level == 0) flush_lock.lock();}
  void inline unlock_flush() {if (level == 0) flush_lock.unlock();}

  void inline lock_rw()   {if (level == 0) RW_lock.lock();}
  void inline unlock_rw() {if (level == 0) RW_lock.unlock();}

  void validate_write(char *data, uint32_t size);

  inline bool is_leaf()                     {return min_key == max_key;}
  inline void set_size(File_Pointer npos=0) {storage_ptr = npos;}
  inline buffer_id_t get_id()               {return id;}
  inline File_Pointer size()                {return storage_ptr;}
  inline File_Pointer offset()              {return file_offset;}

  inline void add_child(buffer_id_t child) {
    children_num++;
    first_child = (first_child == 0)? child : first_child;
  }

  inline void print() {
    printf("buffer %u: storage_ptr = %lu, offset = %lu, min_key=%u, max_key=%u, first_child=%u, #children=%u\n", 
      id, storage_ptr, file_offset, min_key, max_key, first_child, children_num);
  }

  static std::condition_variable buffer_ready;
  static std::mutex buffer_ready_lock;
};

class BufferNotLockedError : public std::exception {
private:
  buffer_id_t id;
public:
  explicit BufferNotLockedError(buffer_id_t id) : id(id){}
  const char* what() const noexcept override {
    std::string temp = "Buffer not locked! id: " + std::to_string(id);
    // ok, since this will terminate the program anyways
    return temp.c_str();
  }
};
