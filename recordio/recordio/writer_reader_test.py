import unittest
import tempfile
from recordio.recordio.header import Compressor
from recordio.recordio.writer import Writer
from recordio.recordio.reader import Reader


class TestHeader(unittest.TestCase):
    """ Test writer.py and reader.py
    """

    def test_write_reader_no_flush(self):
        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, 1000, Compressor(1))
            writer.write(b'china')
            writer.write(b'usa')
            writer.write(b'russia')
            writer.flush()

            tmp_file.seek(0)
            reader = Reader(tmp_file, 0)
            self.assertEqual(3, reader.total_count())
            while reader.has_next():
                reader.next()

    def test_write_reader_auto_flush(self):
        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, 10, Compressor(1))
            writer.write(b'china')
            writer.write(b'usa')
            writer.write(b'russia')
            writer.flush()

            tmp_file.seek(0)
            reader = Reader(tmp_file, 0)
            self.assertEqual(2, reader.total_count())
            while reader.has_next():
                reader.next()


if __name__ == '__main__':
    unittest.main()
