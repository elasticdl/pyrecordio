import unittest
import tensorflow as tf
from recordio.tensorflow_op.python.tf_recordio_dataset import RecordIODataset 

filename = 'recordio/tensorflow_op/python/test_data/test_data_with_snappy_compression'

class TestRecordioDataset(unittest.TestCase):
    """ Test tf_recordio_dataset_test.py
    """

    def test_read_dataset_first_chunk(self):
        expected_data = [
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
        offset = 0
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for _ in range(3):
                record = sess.run(one_element) 
                chunk_data.append(record)

        self.assertEqual(list(expected_data[0:3]), list(chunk_data))

    def test_read_dataset_second_chunk(self):
        expected_data = [
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
        offset = 48
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for i in range(2):
                record = sess.run(one_element)
                chunk_data.append(record)
        self.assertEqual(list(expected_data[3:5]), list(chunk_data))
 
    def test_read_dataset_last_chunk(self):
        expected_data = [
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
        offset = 439
        dataset = RecordIODataset(filename, offset)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        chunk_data = []

        with tf.Session('') as sess:
            for i in range(2):
                record = sess.run(one_element)
                chunk_data.append(record)
        self.assertEqual(list(expected_data[18:20]), list(chunk_data))

if __name__ == '__main__':
    unittest.main()
