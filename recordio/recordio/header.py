from enum import Enum
from .global_variables import int_word_len, endian
import os


class Header(object):
    """Header of recordio file.
    """

    MAGIC_NUMBER = 0x01020304

    def __init__(self, *, num_records, checksum, compressor, compress_size):
        # Total record number of the chunk.
        self._num_records = num_records
        # Checksum of the chunk.
        self._checksum = checksum
        # Compression algorithm applied to chunk.
        self._compressor = compressor
        # The size of the chunk after compression.
        self._compress_size = compress_size

    def write(self, out_file):
        """ Flush the header content to the output file.

        Arguments:
            out_file: The destination file.
        """
        out_file.write(Header.MAGIC_NUMBER.to_bytes(int_word_len, endian))
        out_file.write(self._num_records.to_bytes(int_word_len, endian))
        out_file.write(self._checksum.to_bytes(int_word_len, endian))
        out_file.write(self._compressor.value.to_bytes(int_word_len, endian))
        out_file.write(self._compress_size.to_bytes(int_word_len, endian))

    def checksum(self):
        """ Return the checksum of the data bytes in the chunk

        Returns:
            the checksum
        """
        return self._checksum

    def compressor(self):
        """ Return the compressor of the chunk

        Returns:
            the compressor
        """
        return self._compressor

    def total_count(self):
        """ Return the total record count of the chunk

        Returns:
            total record count
        """
        return self._num_records

    def compress_size(self):
        """ Return the total bytes size of the compressed data in the chunk

        Returns:
            total data bytes size
        """
        return self._compress_size

    @staticmethod
    def parse(in_file, offset):
        """ Read and parse header content from input file

        Arguments:
            in_file: The source file.
            offset: The header start offset in the file. 

        Raises:
            ValueError: invalid offset.
        
        Returns:
            A constructed Header object.
        """

        file_size = os.path.getsize(in_file.name)
        if offset < 0 or offset >= (file_size - int_word_len - 1):
            raise ValueError(
                "invalid offset {} and total file size {}".format(
                    offset, file_size
                )
            )

        in_file.seek(offset)

        magic_number = int.from_bytes(in_file.read(int_word_len), endian)
        if magic_number != Header.MAGIC_NUMBER:
            raise ValueError(
                "Wrong magic number in header, possible file corruption. "
                "Offset: {}".format(offset)
            )

        num_records = int.from_bytes(in_file.read(int_word_len), endian)
        checksum = int.from_bytes(in_file.read(int_word_len), endian)
        compressor = Compressor(
            int.from_bytes(in_file.read(int_word_len), endian)
        )
        compress_size = int.from_bytes(in_file.read(int_word_len), endian)

        return Header(
            num_records=num_records,
            checksum=checksum,
            compressor=compressor,
            compress_size=compress_size,
        )


class Compressor(Enum):
    """Compression algorithm used for recordio file"""

    # Store the raw data without any compression.
    no_compression = 1
    # Compression algorithm from google which aims for very high speeds and
    # reasonable compression.
    snappy = 2
    # Compression algorithm with superior compression ratio.
    gzip = 3
