import unittest
import tensorflow as tf
from tf_recordio_dataset import RecordIODataset


class TestRecordioDataset(unittest.TestCase):
    """ Test tf_recordio_dataset_test.py
    """

    def test_dataset(self):
        filenames = '../test/data/test_data_with_snappy_compression'
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
        dataset = RecordIODataset(filenames)
        iterator = dataset.make_one_shot_iterator()
        one_element = iterator.get_next()
        input_data = []

        with tf.Session('') as sess:
            for i in range(20):
                record = sess.run(one_element) 
                input_data.append(record.decode('utf-8'))

        self.assertEqual(list(expected_data), list(input_data))

if __name__ == '__main__':
    unittest.main()
