#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

Status Chunk::Next(string* record) {
  if (cur_record_idx_ >= records_.size()) {
    return errors::OutOfRange("record index out of range");
  }
  string src_record = records_.at(cur_record_idx_++);
  record->clear();
  record->resize(src_record.size());
  char* result_buffer = &(*record)[0];
  memmove(result_buffer, src_record.data(), src_record.size());
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
