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

    ChunkParser() {}

    ~ChunkParser() = default;

    // Parse the next chunk block in the `input_stream` starting from `offset`.
    Status Parse(InputStreamInterface* input_stream,
                 size_t* offset, 
                 std::vector<std::string>* records);
};
}  // namespace io
}  // namespace tensorflow

#endif // ELASTICDL_RECORDIO_CHUNK_PARSER_H_
