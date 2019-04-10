import gzip
import os
from crc32c import crc32
import snappy
from .header import Header, Compressor
from .global_variables import int_word_len, endian


class Chunk(object):
    """ A chunk is part of the original input file and is composed of one or more records
    """

    @staticmethod
    def parse(in_file, offset):
        """ Read and parse a chunk from input file at offset.

        Arguments:
            in_file: The input file contains the original data.
            offset: The chunk start offset in the file. 

        Returns:
            The parsed chunk.

        Raises:
            ValueError: invalid offset.
            RuntimeError: checksum check failed. 
            ValueError: invalid compressor.
        """
        file_size = os.path.getsize(in_file.name)
        if offset < 0 or offset >= (file_size - int_word_len - 1):
            raise ValueError(
                'invalid offset {}, total file size {}'.format(
                    offset, file_size))

        in_file.seek(offset)

        header = Header.parse(in_file, offset)
        compressed_byte_arr = in_file.read(header.compress_size())
        uncompressed_byte_arr = None

        real_checksum = crc32(compressed_byte_arr)
        raw_checksum = header.checksum()

        if real_checksum != raw_checksum:
            raise RuntimeError(
                "checksum check failed for raw checksum {} and new checksum {}".format(
                    raw_checksum, real_checksum))

        compressor = header.compressor()
        # No compression
        if compressor is Compressor.no_compression:
            uncompressed_byte_arr = compressed_byte_arr
        # Snappy
        elif compressor is Compressor.snappy:
            uncompressed_byte_arr = snappy.uncompress(compressed_byte_arr)
        # Gzip
        elif compressor is Compressor.gzip:
            uncompressed_byte_arr = gzip.decompress(compressed_byte_arr)
        else:
            raise ValueError('invalid compressor')

        curr_index = 0
        s_index = 0
        e_index = 0
        total_bytes = 0

        chunk = Chunk()
        for _ in range(header.total_count()):
            rc_len = int.from_bytes(
                uncompressed_byte_arr[curr_index:curr_index + int_word_len], endian)
            s_index = curr_index + int_word_len
            e_index = s_index + rc_len
            total_bytes += rc_len

            # Read real data
            chunk.add(uncompressed_byte_arr[s_index:e_index])
            curr_index += int_word_len
            curr_index += rc_len

        return chunk

    def __init__(self):
        # Current records stored in this chunk
        self._records = []
        # Total byte size of current records
        self._num_bytes = 0

    def add(self, record):
        """ Add a new string record value to this chunk

        Arguments:
            record: A python3 byte class representing a record
        """
        if not isinstance(record, bytes):
            raise ValueError('Expect bytes type, got: ' + type(record))
            
        self._num_bytes += len(record)
        self._records.append(record)

    def get(self, index):
        """ Get a record string at the specified index position

        Arguments:
            index: The position of record in the records

        Returns:
            A string value represending the specified record
        """
        return self._records[index]

    def write(self, out_file, compressor):
        """ Write the chunk to the output file.

        Arguments:
            out_file: The output file of recordio format.
            compressor: The compressor enum.

        Returns:
            True if the write operation execute successfully.

        Raises:
            ValueError: invalid compressor
        """
        if not self._records:
            return True

        # Compress the data according to compressor type
        uncompressed_bytes = bytearray()
        for record in self._records:
            rc_len = len(record)
            len_bytes = rc_len.to_bytes(int_word_len, endian)
            for lbyte in len_bytes:
                uncompressed_bytes.append(lbyte)
            for dbyte in record:
                uncompressed_bytes.append(dbyte)

        compressed_data = None

        # No compression
        if compressor is Compressor.no_compression:
            compressed_data = uncompressed_bytes
        # Snappy
        elif compressor is Compressor.snappy:
            compressed_data = snappy.compress(uncompressed_bytes)
        # Gzip
        elif compressor is Compressor.gzip:
            compressed_data = gzip.compress(uncompressed_bytes)
        # By default
        else:
            raise ValueError('invalid compressor')

        # Write chunk header into output file
        checksum = crc32(compressed_data)
        header = Header(
            num_records=self.total_count(),
            checksum=checksum,
            compressor=compressor,
            compress_size=len(compressed_data))
        header.write(out_file)

        # Write the compressed data body
        out_file.write(compressed_data)

        return True

    def total_count(self):
        """ Return current total records in the chunk

        Returns:
            current total records size
        """
        return len(self._records)

    def num_bytes(self):
        """ Return current total bytes size in the chunk

        Returns:
            current total bytes size
        """
        return self._num_bytes
