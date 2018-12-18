#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

Status Chunk::Next(string* record) {
  if (cur_record_idx_ >= records_.size()) {
    return errors::OutOfRange("record index out of range");
  }
  record->swap(records_[cur_record_idx_++]);
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
