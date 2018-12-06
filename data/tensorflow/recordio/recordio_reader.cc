#include "tensorflow/core/lib/io/random_inputstream.h"
#include "recordio_reader.h"

namespace tensorflow {
namespace io {

RecordIOReader::RecordIOReader(RandomAccessFile* file,
                               size_t offset)
    : input_stream_(new RandomAccessInputStream(file)),
      offset_(offset) {
  // Parse the chunk. 
  std::vector<std::string> records;
  parser_.Parse(input_stream_.get(), &offset_, records);
  chunk_.reset(new Chunk(records));
}

Status RecordIOReader::ReadRecord(string* record) {
  // Fetch the next record.
  Status s = chunk_->Next(record);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
