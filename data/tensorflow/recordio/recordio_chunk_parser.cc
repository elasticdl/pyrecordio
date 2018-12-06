#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/core/coding.h"
#include "snappy.h"

#include "recordio_chunk_parser.h"

namespace tensorflow {
namespace io {

Status ChunkParser::ReadNBytes(InputStreamInterface* input_stream,
                         uint64 offset,
                         size_t n,
                         string* result) {
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

Status ChunkParser::Parse(InputStreamInterface* input_stream,
                    uint64* offset,
                    std::vector<std::string>& records) {
  // Reset file offset for chunk processing.
  input_stream->Reset();
  input_stream->SkipNBytes(*offset);

  // Read chunk header from `input_stream`.
  string chunk_header;
  TF_RETURN_IF_ERROR(ReadNBytes(input_stream, *offset, kHeaderSize_, &chunk_header));

  const char *hdr_data= chunk_header.data();
  const uint32 num_records_ = core::DecodeFixed32(hdr_data + sizeof(uint32));
  const uint32 checksum = core::DecodeFixed32(hdr_data + sizeof(uint32) * 2);
  compression_type_ = CompressionType(
      core::DecodeFixed32(hdr_data + sizeof(uint32) * 3));
  const uint32 compress_size = core::DecodeFixed32(hdr_data + sizeof(uint32) * 4);

  *offset += kHeaderSize_;

  // Read chunk Data.
  string chunk_data;
  string decompressed_data;
  const char* data = nullptr;
  TF_RETURN_IF_ERROR(ReadNBytes(input_stream, *offset, compress_size, &chunk_data));
  *offset += compress_size;

  // CRC32 check.
  if (checksum != crc32c::Value(chunk_data.data(), compress_size)) {
    return errors::DataLoss("corrupted record");
  }

  // Decompress chunk data using specified algorithm.
  switch (core::DecodeFixed32(hdr_data + sizeof(uint32) * 3)) {
    case SNAPPY: {
      SnappyDecompress(&chunk_data, &decompressed_data);
      data = decompressed_data.data();
      break;
    }
    case GZIP: {
      GzipDecompress(&chunk_data, &decompressed_data);
      data = decompressed_data.data();
      break;
    }
    case ZLIB: {
      ZlibDecompress(&chunk_data, &decompressed_data);
      data = decompressed_data.data();
      break;
    }
    default: {
      data = chunk_data.data();
    }
  }

  // Parse the decompressed chunk data.
  uint32 record_offset = 0;
  for (int i = 0; i < num_records_; ++i) {
    const uint32 record_len =  core::DecodeFixed32(data + record_offset);
    record_offset += sizeof(uint32);
    const string &record = decompressed_data.substr(record_offset, record_len);
    records.emplace_back(record);
    record_offset += record_len;
  }
  return Status::OK();
}

Status ChunkParser::GzipDecompress(const string* src_str, const string* dst_str) {
  // TODO
  return errors::Unimplemented("");
}

Status ChunkParser::ZlibDecompress(const string* src, const string* dst) {
  // TODO
  return errors::Unimplemented("");
}

Status ChunkParser::SnappyDecompress(const string* src, string* dst) {
  snappy::Uncompress(src->data(), src->size(), dst);
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
