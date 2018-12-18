import unittest
import tempfile
from recordio.recordio.file import File


class TestRecordIOFile(unittest.TestCase):
    """ Test file.py
    """

    def test_read_by_index(self):
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

        tmp_file = tempfile.NamedTemporaryFile()

        with File(tmp_file.name, 'w') as rdio_w:
            for data in data_source:
                rdio_w.write(data)

        with File(tmp_file.name, 'r') as rdio_r:
            self.assertEqual(list(rdio_r), list(data_source))

    def test_read_by_iter(self):
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

        # this tmp file will be closed in File.close()
        tmpfile_name = tempfile.NamedTemporaryFile().name
        with File(tmpfile_name, 'w') as rdio_w:
            for data in data_source:
                rdio_w.write(data)

        with File(tmpfile_name, 'r') as rdio_r:
            self.assertEqual(list(rdio_r), list(data_source))


if __name__ == '__main__':
    unittest.main()
