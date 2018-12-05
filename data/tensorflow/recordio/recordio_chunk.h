#ifndef ELASTICDL_RECORDIO_CHUNK_H_
#define ELASTICDL_RECORDIO_CHUNK_H_

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/io/inputstream_interface.h"

namespace tensorflow {
namespace io {

class Chunk {
  public:
    Chunk(const std::vector<std::string>& records)
        : num_records_(0), 
          cur_record_idx_(0),
          records_(records) {}
    
    virtual ~Chunk() = default;

    // Return true if there is still record in the current chunk data.
    bool HasNext();

    // Return the next record in the current chunk.
    Status Next(string* record);

  private:
    // Total number of records in current chunk.
    uint32 num_records_;

    // Current processing record index of the chunk. 
    size_t cur_record_idx_;
 
    // All the record data in the current chunk. 
    std::vector<std::string> records_;

    TF_DISALLOW_COPY_AND_ASSIGN(Chunk);
};
}  // namespace io
}  // namespace tensorflow

#endif // ELASTICDL_RECORDIO_CHUNK_H_
