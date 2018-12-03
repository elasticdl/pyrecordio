#ifndef ELASTICDL_RECORDIO_CHUNK_H_
#define ELASTICDL_RECORDIO_CHUNK_H_

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/io/inputstream_interface.h"

namespace tensorflow {
namespace io {

class Chunk {
  public:
    // Compression algorithm which applied to every single chunk data.
    enum CompressionType { NONE = 1, SNAPPY = 2, GZIP = 3, ZLIB = 4, };

    Chunk()
        : num_records_(0), 
          cur_record_idx_(0), 
          compression_type_(NONE) {}
    
    virtual ~Chunk() = default;

    // Called before go to parse the next chunk in a recordio file.
    Status Reset();

    // Parse the next chunk block in the `input_stream` starting from `offset`.
    Status Parse(std::unique_ptr<InputStreamInterface> &input_stream, 
                 uint64* offset);

    // Return true if there is still record in the current chunk data.
    bool HasNext();

    // Return the next record in the current chunk.
    Status Next(string* record);

  private:
    // Read `n` bytes from the `offset` into `result` string.
    Status ReadNBytes(std::unique_ptr<InputStreamInterface> &input_stream,
                      uint64 offset, 
                      size_t n, 
                      string* result);

    // Decompress the `src` input data into `dst` output using gzip algorithm.
    Status GzipDecompress(const string* src, const string* dst);

    // Decompress the `src` input data into `dst` output using zlib algorithm.
    Status ZlibDecompress(const string* src, const string* dst);

    // Decompress the `src` input data into `dst` output using snappy algorithm.
    Status SnappyDecompress(const string* src, string* dst);

    // Chunk header size.
    static const uint32 kHeaderSize_ = sizeof(uint32) * 5;

    // Total number of records in current chunk.
    uint32 num_records_;

    // Current processing record index of the chunk. 
    size_t cur_record_idx_;
    CompressionType compression_type_;
 
    // All the record data in the current chunk. 
    std::vector<std::string> records_;

    TF_DISALLOW_COPY_AND_ASSIGN(Chunk);
};
}  // namespace io
}  // namespace tensorflow

#endif // ELASTICDL_RECORDIO_CHUNK_H_
