#ifndef ELASTICDL_RECORDIO_READER_H_
#define ELASTICDL_RECORDIO_READER_H_

#include "recordio_chunk.h"
#include "recordio_chunk_parser.h"

namespace tensorflow {

class RandomAccessFile;

namespace io {

class RecordIOReader {
 public:
  RecordIOReader(RandomAccessFile* file,
                 size_t offset);

  ~RecordIOReader() = default;

  // Reads the next record in the recordio file into *record. 
  // Returns OK on success.
  Status ReadRecord(string* record);

  // Returns the current offset of the specified chunk.
  uint64 TellOffset() { return offset_; }

  // Reset the logic offset of recordio. 
  // Trying to seek backward will throw error.
  Status SeekOffset(uint64 offset) {
    if (offset < offset_)
      return errors::InvalidArgument(
          "Trying to seek offset: ", offset,
          " which is less than the current offset: ", offset_);
    offset_ = offset;
    return Status::OK();
  }

 private:
  // Input stream for recordio file.
  std::unique_ptr<InputStreamInterface> input_stream_;

  // Current read offset of the specified chunk.
  size_t offset_;

  Status init_status_;

  // Current processing chunk.
  std::unique_ptr<Chunk> chunk_; 
  ChunkParser parser_;

  TF_DISALLOW_COPY_AND_ASSIGN(RecordIOReader);
};

}  // namespace io
}  // namespace tensorflow

#endif  // ELASTICDL_RECORDIO_READER_H_
