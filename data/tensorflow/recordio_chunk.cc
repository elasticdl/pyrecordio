#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/core/coding.h"
#include "snappy.h"
#include "recordio_chunk.h"

namespace tensorflow {
namespace io {

Status Chunk::ReadNBytes(std::unique_ptr<InputStreamInterface> &input_stream, 
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

Status Chunk::Parse(std::unique_ptr<InputStreamInterface> &input_stream,
                    uint64* offset) {
  // Read chunk header from `input_stream`.
  string chunk_header;
  ReadNBytes(input_stream, *offset, kHeaderSize_, &chunk_header);

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
  ReadNBytes(input_stream, *offset, compress_size, &chunk_data);
  *offset += compress_size;

  // CRC32 check.
  if (checksum != crc32c::Value(chunk_data.data(), compress_size)) {
    return errors::DataLoss("corrupted record");
  }

  // Decompress chunk data using specified algorithm.
  if (compression_type_ == SNAPPY) {
    SnappyDecompress(&chunk_data, &decompressed_data);
    data = decompressed_data.data();
  }
  else if (compression_type_ == GZIP) {
    GzipDecompress(&chunk_data, &decompressed_data);
    data = decompressed_data.data();
  }
  else if (compression_type_ == ZLIB) {
    ZlibDecompress(&chunk_data, &decompressed_data);
    data = decompressed_data.data();
  }
  else {
    data = chunk_data.data();
  }

  // Parse the decompressed chunk data.
  uint32 record_offset = 0;
  for (int i = 0; i < num_records_; ++i) {
    const uint32 record_len =  core::DecodeFixed32(data + record_offset); 
    record_offset += sizeof(uint32);
    string record = decompressed_data.substr(record_offset, record_len);
    records_.emplace_back(record);
    record_offset += record_len;
  }
  return Status::OK();
}

Status Chunk::Reset() {
  records_.clear();
  cur_record_idx_ = 0;
  return Status::OK();
}

bool Chunk::HasNext() {
  return cur_record_idx_ < records_.size();
}

Status Chunk::Next(string* record) {
  string src_record = records_.at(cur_record_idx_++);
  record->clear();
  record->resize(src_record.size());
  char* result_buffer = &(*record)[0];
  memmove(result_buffer, src_record.data(), src_record.size());
  return Status::OK();
}

Status Chunk::GzipDecompress(const string* src_str, const string* dst_str) {
  // TODO
  return errors::Unimplemented("");
}

Status Chunk::ZlibDecompress(const string* src, const string* dst) {
  // TODO
  return errors::Unimplemented("");
}

Status Chunk::SnappyDecompress(const string* src, string* dst) {
  snappy::Uncompress(src->data(), src->size(), dst);
  return Status::OK();
}

}  // namespace io
}  // namespace tensorflow
