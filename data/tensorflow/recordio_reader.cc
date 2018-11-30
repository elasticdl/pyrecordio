#include "tensorflow/core/lib/io/random_inputstream.h"
#include "recordio_reader.h"

namespace tensorflow {
namespace io {

RecordIOReader::RecordIOReader(RandomAccessFile* file,
                               uint64 file_size,
                               size_t start_chunk,
                               size_t desired_chunknum)
    : input_stream_(new RandomAccessInputStream(file)),
      offset_(0),
      start_chunk_(start_chunk),
      desired_chunknum_(desired_chunknum),
      cur_chunknum_(0),      
      file_size_(file_size) {
  // Initialize the file index.
  file_index_.Parse(input_stream_, file_size_);
  size_t chunk_offset = file_index_.GetChunkOffset(start_chunk_ - 1);
  offset_ = chunk_offset;
  if (desired_chunknum_ <= 0) {
    desired_chunknum_ = file_index_.GetTotalChunkNum();
  }

  // Reset file offset for chunk processing.
  input_stream_->Reset();
  input_stream_->SkipNBytes(chunk_offset);
}

Status RecordIOReader::ReadRecord(string* record) {
  // Reach end of current chunk and go to the next chunk if any.
  // It is RecordIOReader who is responsible for handling multi-chunks 
  // in single recordio file.
  if (ReachEndOfChunk()) {
    chunk_.Reset();
    chunk_.Parse(input_stream_, &offset_);
    ++cur_chunknum_;

    // Check the offset for each chunk.
    DCHECK_EQ(offset_, input_stream_->Tell());
  }

  // Fetch the next record.
  Status s = chunk_.Next(record);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
