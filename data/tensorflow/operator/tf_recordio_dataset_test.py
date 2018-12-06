import unittest
import tensorflow as tf
from tf_recordio_dataset import RecordIODataset


class TestRecordioDataset(unittest.TestCase):
    """ Test tf_recordio_dataset_test.py
    """

    def test_read_dataset_first_chunk(self):
        filename = '../test/data/test_data_with_snappy_compression'
        expected_data = [
            'china',
            'usa',
            'russia',
            'india',
            'thailand',
            'finland',
            'france',
            'germany',
            'poland',
            'san marino',
            'sweden',
            'neuseeland',
            'argentina',
            'canada',
            'ottawa',
            'bogota',
            'panama',
            'united states',
            'brazil',
            'barbados']
        offset = 0
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for i in range(3):
                record = sess.run(one_element) 
                chunk_data.append(record.decode('utf-8'))

        self.assertEqual(list(expected_data[0:3]), list(chunk_data))

    def test_read_dataset_second_chunk(self):
        filename = '../test/data/test_data_with_snappy_compression'
        expected_data = [
            'china',
            'usa',
            'russia',
            'india',
            'thailand',
            'finland',
            'france',
            'germany',
            'poland',
            'san marino',
            'sweden',
            'neuseeland',
            'argentina',
            'canada',
            'ottawa',
            'bogota',
            'panama',
            'united states',
            'brazil',
            'barbados']
        offset = 48
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for i in range(2):
                record = sess.run(one_element)
                chunk_data.append(record.decode('utf-8'))
        self.assertEqual(list(expected_data[3:5]), list(chunk_data))
 
    def test_read_dataset_last_chunk(self):
        filename = '../test/data/test_data_with_snappy_compression'
        expected_data = [
            'china',
            'usa',
            'russia',
            'india',
            'thailand',
            'finland',
            'france',
            'germany',
            'poland',
            'san marino',
            'sweden',
            'neuseeland',
            'argentina',
            'canada',
            'ottawa',
            'bogota',
            'panama',
            'united states',
            'brazil',
            'barbados']
        offset = 439
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for i in range(2):
                record = sess.run(one_element)
                chunk_data.append(record.decode('utf-8'))
        self.assertEqual(list(expected_data[18:20]), list(chunk_data))

if __name__ == '__main__':
    unittest.main()
