import unittest
import codecs
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

def write_recordio_file(encoder=None):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    with File(tmp_file.name, 'w', encoder=encoder) as rdio_w:
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

    def test_encoder(self):
        encoder = lambda x : codecs.encode(x.decode(), "rot_13").encode()
        file_name = write_recordio_file(encoder=encoder)
        with File(file_name, 'r') as rdio_r:
            self.assertEqual(list(rdio_r), list(map(encoder, data_source)))

    def test_decoder(self):
        encoder = lambda x : codecs.encode(x.decode(), "rot_13").encode()
        file_name = write_recordio_file(encoder=encoder)
        decoder = lambda x : codecs.decode(x.decode(), "rot_13").encode()
        with File(file_name, 'r', decoder=decoder) as rdio_r:
            self.assertEqual(list(rdio_r), data_source)

if __name__ == '__main__':
    unittest.main()
