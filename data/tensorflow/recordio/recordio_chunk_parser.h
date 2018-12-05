#ifndef ELASTICDL_RECORDIO_CHUNK_PARSER_H_
#define ELASTICDL_RECORDIO_CHUNK_PARSER_H_

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/io/inputstream_interface.h"
#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

class ChunkParser {
  public:
    // Compression algorithm which applied to every single chunk data.
    enum CompressionType { NONE = 1, SNAPPY = 2, GZIP = 3, ZLIB = 4, };

    ChunkParser() : compression_type_(NONE) {}

    ~ChunkParser() = default;

    // Parse the next chunk block in the `input_stream` starting from `offset`.
    Status Parse(InputStreamInterface* input_stream,
                 uint64* offset, 
                 std::vector<std::string>& records);

  private:
    // Read `n` bytes from the `offset` into `result` string.
    // TODO: add MUST_USE_RESULT using 
    // `https://github.com/abseil/abseil-cpp/blob/master/absl/base/attributes.h`
    Status ReadNBytes(InputStreamInterface* input_stream,
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

    CompressionType compression_type_;
};
}  // namespace io
}  // namespace tensorflow

#endif // ELASTICDL_RECORDIO_CHUNK_PARSER_H_
