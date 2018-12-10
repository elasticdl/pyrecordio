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
  init_status_ = parser_.Parse(input_stream_.get(), &offset_, &records);
  if (init_status_.ok()) {
    chunk_.reset(new Chunk(std::move(records)));
  }
}

Status RecordIOReader::ReadRecord(string* record) {
  if (!init_status_.ok()) {
    return init_status_;
  }
  // Fetch the next record.
  return chunk_->Next(record);
}

}  // namespace io
}  // namespace tensorflow
