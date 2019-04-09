import unittest
import tempfile
from .header import Compressor
from .file_index import FileIndex
from .writer import Writer


class TestWriter(unittest.TestCase):
    """ Test writer.py and ensure the correctness of index.
    """

    def write_and_get_index(self, *, chunksize):
        data = [b'china', b'usa', b'russia']
        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, chunksize, Compressor(1))
            for d in data:
                writer.write(d)
            writer.flush()

            tmp_file.seek(0)
            return FileIndex(tmp_file)

    def test_write_no_flush(self):
        # Use a large chunksize, so no auto flush happens.
        index = self.write_and_get_index(chunksize = 1000)
        self.assertEqual(1, index.total_chunks())
        self.assertEqual(3, index.total_records())
        self.assertEqual(3, index.chunk_records(0))

    def test_write_reader_auto_flush(self):
        # Use a small chunksize, so auto flush happens.
        index = self.write_and_get_index(chunksize = 10)
        self.assertEqual(2, index.total_chunks())
        self.assertEqual(3, index.total_records())
        self.assertEqual(2, index.chunk_records(0))
        self.assertEqual(1, index.chunk_records(1))


if __name__ == '__main__':
    unittest.main()
