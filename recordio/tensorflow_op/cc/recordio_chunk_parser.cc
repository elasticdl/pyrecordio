#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/core/coding.h"
#include "snappy.h"

#include "recordio_chunk_parser.h"

namespace tensorflow {
namespace io {
namespace {
Status ReadNBytes(InputStreamInterface* input_stream,
                         size_t offset,
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

Status GzipDecompress(const string* src_str, string* dst_str) {
  // TODO
  return errors::Unimplemented("");
}

Status ZlibDecompress(const string* src, string* dst) {
  // TODO
  return errors::Unimplemented("");
}

Status SnappyDecompress(const string* src, string* dst) {
  if (snappy::Uncompress(src->data(), src->size(), dst)) {
    return Status::OK();
  } else {
    return errors::DataLoss("Snappy decompress error");
  }
}
}

Status ChunkParser::Parse(InputStreamInterface* input_stream,
                    size_t* offset,
                    std::vector<std::string>* records) {
  const uint32 kHeaderSize = sizeof(uint32) * 5;

  // Reset file offset for chunk processing.
  input_stream->Reset();
  input_stream->SkipNBytes(*offset);

  // Read chunk header from `input_stream`.
  string chunk_header;
  TF_RETURN_IF_ERROR(ReadNBytes(input_stream, *offset, kHeaderSize, &chunk_header));

  const char *hdr_data= chunk_header.data();
  const uint32 num_records = core::DecodeFixed32(hdr_data + sizeof(uint32));
  const uint32 checksum = core::DecodeFixed32(hdr_data + sizeof(uint32) * 2);
  const uint32 compress_size = core::DecodeFixed32(hdr_data + sizeof(uint32) * 4);

  *offset += kHeaderSize;

  // Read chunk Data.
  string chunk_data;
  string decompressed_data;
  TF_RETURN_IF_ERROR(ReadNBytes(input_stream, *offset, compress_size, &chunk_data));
  *offset += compress_size;

  // CRC32 check.
  if (checksum != crc32c::Value(chunk_data.data(), compress_size)) {
    return errors::DataLoss("corrupted record");
  }

  // Decompress chunk data using specified algorithm.
  Status status;
  int compression = core::DecodeFixed32(hdr_data + sizeof(uint32) * 3);
  switch (compression) {
    case SNAPPY: {
      status = SnappyDecompress(&chunk_data, &decompressed_data);
      break;
    }
    case GZIP: {
      status = GzipDecompress(&chunk_data, &decompressed_data);
      break;
    }
    case ZLIB: {
      status = ZlibDecompress(&chunk_data, &decompressed_data);
      break;
    }
    case NONE: {
      chunk_data.swap(decompressed_data);
      break;
    }
    default:
      status = errors::Unimplemented("Unimplemented compression: ", compression);
  }
  if (!status.ok()) {
    return status;
  }

  // Parse the decompressed chunk data.
  const char* data = decompressed_data.data();
  uint32 record_offset = 0;
  records->resize(num_records);
  for (uint32 i = 0; i < num_records; ++i) {
    const uint32 record_len =  core::DecodeFixed32(data + record_offset);
    record_offset += sizeof(uint32);
    decompressed_data.substr(record_offset, record_len).swap((*records)[i]);
    record_offset += record_len;
  }
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
