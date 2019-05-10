from .file_index import FileIndex
from .writer import Writer
from .reader import RangeReader
from .header import Compressor


class File(object):
    """ Simple Wrapper for FileIndex, Writer and Reader for usability.
    """

    def __init__(self, file_path, mode, *, max_chunk_size=1024,
        compressor=Compressor.snappy, encoder=None, decoder=None):
        """ Initialize according open mode

        Raises:
            ValueError: invalid open mode input param.

        For writes, content bytes will first be encoded by encoder, then
        compressed by compressor, then written to file as a record.

        For reads, a record will first be read from file, then decompressed
        by compressor, then decoded by decoder, then returned to caller.

        Note that unless decoder is a reverse function of encoder, the content
        read back may not be the same as the content written.
        """
        if mode == 'r' or mode == 'read':
            self._mode = 'r'
            self._data = open(file_path, 'rb')
            self._index = FileIndex(self._data)
        elif mode == 'w' or mode == 'write':
            self._mode = 'w'
            self._data = open(file_path, 'wb')
            self._writer = Writer(self._data, max_chunk_size)
        else:
            raise ValueError('mode value should be \'read\' or \'write\'')
        
        self._encoder = encoder
        self._decoder = decoder

    def __enter__(self):
        """ For `with` statement
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ For `with` statement
        """
        self.close()

    def __iter__(self):
        """ For iterate operation

        Returns:
            Iterator of dataset
        """
        return self.get_reader()

    def get_reader(self, start=0, end=None):
        """Return a reader for the given range."""

        if self._mode != 'r':
            raise RuntimeError('Should be under read mode')

        it = RangeReader(self._data, self._index, start, end)
        return map(self._decoder, it) if self._decoder else it

    def write(self, record):
        """ Write a record into recordio file.

        Arguments:
            record: Record value String.

        Raises:
            RuntimeError: wrong open mode.
        """
        if self._mode != 'w':
            raise RuntimeError('Should be under write mode')

        encoded = self._encoder(record) if self._encoder else record
        self._writer.write(encoded)

    def close(self):
        """ Close the data file
        """
        if self._mode == 'w':
            self._writer.flush()
        self._data.close()

    def get(self, index):
        """ Get the record string value specified by index

        Arguments:
            index: record index in the recordio file

        Returns:
            Record string value

        Raises:
            RuntimeError: wrong open mode.
        """
        if self._mode != 'r':
            raise RuntimeError('Should be under read mode')

        reader = self.get_reader(index, index + 1)
        try:
            return next(reader)
        except StopIteration:
            raise IndexError()

    def count(self):
        """ Return total record count of the recordio file

        Returns:
            Total record count

        Raises:
            RuntimeError: wrong open mode.
        """
        if self._mode != 'r':
            raise RuntimeError('Should be under read mode')

        return self._index.total_records()
