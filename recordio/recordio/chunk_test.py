import unittest
import tempfile
from .chunk import Chunk
from .header import Compressor


class TestHeader(unittest.TestCase):
    """ Test chunk.py
    """

    def test_add_and_get(self):
        chunk = Chunk()
        record1 = b'china'
        record2 = b'usa'
        record3 = b'russia'
        chunk.add(record1)
        chunk.add(record2)
        chunk.add(record3)

        self.assertEqual(chunk.get(0), record1)
        self.assertEqual(chunk.get(1), record2)
        self.assertEqual(chunk.get(2), record3)

    def test_write_and_parse(self):
        chunk = Chunk()
        record1 = b'china'
        record2 = b'usa'
        record3 = b'russia'
        chunk.add(record1)
        chunk.add(record2)
        chunk.add(record3)

        with tempfile.NamedTemporaryFile() as tmp_file:
            chunk.write(tmp_file, Compressor(2))
            tmp_file.seek(0)
            chunk.parse(tmp_file, 0)

        self.assertEqual(chunk.get(0), record1)
        self.assertEqual(chunk.get(1), record2)
        self.assertEqual(chunk.get(2), record3)


if __name__ == '__main__':
    unittest.main()
