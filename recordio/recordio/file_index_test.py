import unittest
import tempfile
from recordio.recordio.file_index import FileIndex
from recordio.recordio.header import Compressor
from recordio.recordio.writer import Writer
from recordio.recordio.reader import Reader


class TestFileIndex(unittest.TestCase):
    """ Test file_index.py
    """

    def test_one_chunk(self):
        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, 1000, Compressor(1))
            writer.write(b'china')
            writer.write(b'usa')
            writer.write(b'russia')
            writer.flush()

            tmp_file.seek(0)
            index = FileIndex(tmp_file)

            self.assertEqual(1, index.total_chunks())
            self.assertEqual(3, index.chunk_records(0))

    def test_two_chunk(self):
        with tempfile.NamedTemporaryFile() as tmp_file:
            writer = Writer(tmp_file, 10, Compressor(1))
            writer.write(b'china')
            writer.write(b'usa')
            writer.write(b'russia')
            writer.flush()

            tmp_file.seek(0)
            index = FileIndex(tmp_file)

            self.assertEqual(2, index.chunk_records(0))
            self.assertEqual(1, index.chunk_records(1))
            self.assertEqual(2, index.total_chunks())

    def test_usage(self):
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

            parsed_data = []
            tmp_file.seek(0)
            index = FileIndex(tmp_file)

            for i in range(index.total_chunks()):
                reader = Reader(tmp_file, index.chunk_offset(i))
                while reader.has_next():
                    parsed_data.append(reader.next())

        self.assertEqual(data_source, parsed_data)

    def test_readme_demo(self):
        with tempfile.NamedTemporaryFile() as data:
            max_chunk_size = 1024
            writer = Writer(data, max_chunk_size)
            writer.write(b'abc')
            writer.write(b'edf')
            writer.flush()

            data.seek(0)
            index = FileIndex(data)
            self.assertEqual(2, index.total_records())

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

            chunk = index[7]
            self.assertEqual(chunk.offset, 454)
            self.assertEqual(chunk.len, 39)
            self.assertEqual(chunk.num_record, 2)



if __name__ == '__main__':
    unittest.main()