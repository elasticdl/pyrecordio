import unittest
import tempfile
from .file_index import FileIndex
from .header import Compressor
from .writer import Writer


class TestFileIndex(unittest.TestCase):
    """ Test file_index.py
    """

    def test_locate_record(self):
        data_source = [
            b'china',
            b'usa',
            b'russia',
            b'india',
            b'thailand',
            b'finland',
            b'france',
            b'germany',
            b'poland',
            b'san marino',
            b'sweden',
            b'neuseeland',
            b'argentina',
            b'canada',
            b'ottawa',
            b'bogota',
            b'panama',
            b'united states',
            b'brazil',
            b'barbados']

        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, 20)

            for data in data_source:
                writer.write(data)
            writer.flush()

            tmp_file.seek(0)
            index = FileIndex(tmp_file)

            chunk_idx, record_idx = index.locate_record(0)
            self.assertEqual(chunk_idx, 0)
            self.assertEqual(record_idx, 0)

            chunk_idx, record_idx = index.locate_record(19)
            self.assertEqual(chunk_idx, 7)
            self.assertEqual(record_idx, 1)


if __name__ == '__main__':
    unittest.main()
