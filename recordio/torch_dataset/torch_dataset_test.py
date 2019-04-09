import unittest
import tempfile
from .torch_dataset import TorchDataset 
from recordio import File


class TestTorchDataset(unittest.TestCase):
    """ Test torch_dataset.py
    """

    def test_dataset(self):
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

        with TorchDataset(tmpfile_name) as dataset:
            self.assertEqual(list(dataset), list(data_source))


if __name__ == '__main__':
    unittest.main()
