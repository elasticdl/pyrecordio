import unittest
import tempfile
from .file import File

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

def write_recordio_file():
    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    with File(tmp_file.name, 'w') as rdio_w:
        for data in data_source:
            rdio_w.write(data)
    
    return tmp_file.name


class TestRecordIOFile(unittest.TestCase):
    """ Test file.py
    """

    def test_read_by_index(self):
        file_name = write_recordio_file()
        with File(file_name, 'r') as rdio_r:
            rlist = [rdio_r.get(i) for i in range(len(data_source))]
            self.assertEqual(rlist, data_source)

    def test_read_by_iter(self):
        file_name = write_recordio_file()
        with File(file_name, 'r') as rdio_r:
            self.assertEqual(list(rdio_r), data_source)

if __name__ == '__main__':
    unittest.main()
