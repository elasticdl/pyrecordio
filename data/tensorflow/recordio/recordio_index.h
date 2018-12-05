#ifndef ELASTICDL_RECORDIO_INDEX_H_
#define ELASTICDL_RECORDIO_INDEX_H_

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/io/inputstream_interface.h"

namespace tensorflow {
namespace io {

class FileIndex {
  public:
    FileIndex() : total_chunks_(0) {}

    virtual ~FileIndex() = default;

    // Initialize the index info(eg: offset, data size) of 
    // all the chunks of a single recordio file.
    Status Parse(std::unique_ptr<InputStreamInterface> &input_stream, 
                 uint64 file_size);

    // Return the offset of chunk specified by `chunk_index`.
    uint32 GetChunkOffset(uint32 chunk_index);

    // Return the total number of chunk in the recordio file.
    size_t GetTotalChunkNum(); 

  private:
    // Read `n` bytes from `input_stream` into `result`.
    Status ReadNBytes(std::unique_ptr<InputStreamInterface> &input_stream,
        uint64 offset, size_t n, string* result);

    // Offsets of each chunk in the recordio file.
    std::vector<uint32> chunk_offsets_;

    // Total chunk count of a recordio file
    size_t total_chunks_;

    TF_DISALLOW_COPY_AND_ASSIGN(FileIndex);
};
}  // namespace io
}  // namespace tensorflow

#endif // ELASTICDL_RECORDIO_INDEX_H_
