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

 private:
  // Input stream for recordio file.
  std::unique_ptr<InputStreamInterface> input_stream_;

  Status init_status_;

  // Current processing chunk.
  std::unique_ptr<Chunk> chunk_; 
  ChunkParser parser_;

  TF_DISALLOW_COPY_AND_ASSIGN(RecordIOReader);
};

}  // namespace io
}  // namespace tensorflow

#endif  // ELASTICDL_RECORDIO_READER_H_
