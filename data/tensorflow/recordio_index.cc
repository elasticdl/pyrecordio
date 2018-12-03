#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/core/coding.h"
#include "recordio_index.h"
#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

Status FileIndex::ReadNBytes(
    std::unique_ptr<InputStreamInterface> &input_stream, 
    uint64 offset, size_t n, string* result) {
  TF_RETURN_IF_ERROR(input_stream->ReadNBytes(n, result));
  
  if (result->size() != n) {
    if (result->empty()) {
      return errors::OutOfRange("eof");
    } else {
      return errors::DataLoss("truncated record at ", offset);
    }
  }
  
  return Status::OK();
}

Status FileIndex::Parse(std::unique_ptr<InputStreamInterface> &input_stream, 
                        uint64 file_size) {
  uint64 offset = 0;
  const uint32 header_size = sizeof(uint32) * 5;

  while (offset < file_size) {
    // Read and parse chunk header.
    string chunk_header;
    ReadNBytes(input_stream, offset, header_size, &chunk_header);

    const char *hdr_data= chunk_header.data();
    const uint32 compress_size = core::DecodeFixed32(hdr_data + sizeof(uint32) * 4);

    chunk_offsets_.emplace_back(offset);
    input_stream->SkipNBytes(compress_size);

    offset += header_size + compress_size;
    total_chunks_++;
  }

  return Status::OK();
}

uint32 FileIndex::GetChunkOffset(uint32 chunk_index) {
  return chunk_offsets_.at(chunk_index);
}

size_t FileIndex::GetTotalChunkNum() {
  return total_chunks_;
}

}  // namespace io
}  // namespace tensorflow
