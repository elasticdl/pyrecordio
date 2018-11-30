#ifndef ELASTICDL_RECORDIO_READER_H_
#define ELASTICDL_RECORDIO_READER_H_

#include "recordio_chunk.h"
#include "recordio_index.h"

namespace tensorflow {

class RandomAccessFile;

namespace io {

class RecordIOReader {
 public:
  RecordIOReader(RandomAccessFile* file,
                 uint64 file_size,
                 size_t chunk_index,
                 size_t desired_chunknum);

  virtual ~RecordIOReader() = default;

  // Reads the next record in the recordio file into *record. 
  // Returns OK on success.
  Status ReadRecord(string* record);

  // Returns the current offset in the file.
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
  bool ReachEndOfChunk() {
    return !chunk_.HasNext() && cur_chunknum_ < desired_chunknum_ && 
      cur_chunknum_ < file_index_.GetTotalChunkNum();
  }

  // Input stream for recordio file.
  std::unique_ptr<InputStreamInterface> input_stream_;

  // The size in bytes of recordIO file.
  uint64 file_size_;

  // Current read offset of the recordio file.
  uint64 offset_;

  // The start chunk number(>=1) to read from.
  uint32 start_chunk_;

  // Total desired processing chunk count.
  uint32 desired_chunknum_;

  // Current chunk number already processed.
  size_t cur_chunknum_;

  FileIndex file_index_;

  // Current processing chunk.
  Chunk chunk_;

  TF_DISALLOW_COPY_AND_ASSIGN(RecordIOReader);
};

}  // namespace io
}  // namespace tensorflow

#endif  // ELASTICDL_RECORDIO_READER_H_
