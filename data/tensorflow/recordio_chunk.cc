#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

Status Chunk::Reset() {
  records_.clear();
  cur_record_idx_ = 0;
  return Status::OK();
}

bool Chunk::HasNext() {
  return cur_record_idx_ < records_.size();
}

Status Chunk::Next(string* record) {
  string src_record = records_.at(cur_record_idx_++);
  record->clear();
  record->resize(src_record.size());
  char* result_buffer = &(*record)[0];
  memmove(result_buffer, src_record.data(), src_record.size());
  return Status::OK();
}

Status Chunk::Add(const string& record) {
  records_.emplace_back(record);
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
