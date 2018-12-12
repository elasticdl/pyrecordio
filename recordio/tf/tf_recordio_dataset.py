import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import tensor_shape

try:
  recordio = tf.load_op_library('../../data/tensorflow/operator/librecordio_dataset_op.so')
except:
  recordio = tf.load_op_library('librecordio_dataset_op.so')

class RecordIODataset(tf.data.Dataset):
  """A `Dataset` comprising records from one or more RecordIO files."""

  def __init__(self, filename, offset=0):
    """Creates a `RecordIODataset`.

    Args:
      filename: the absolute path of the recordio file.
      offset: The offset of the chunk to process in the recordio file.
    """
    super(RecordIODataset, self).__init__()
    self._filename = filename
    self._offset = offset

  def _as_variant_tensor(self):
    return recordio.recordio_dataset(self._filename, self._offset)
  
  def _inputs(self):
    return []

  @property
  def output_classes(self):
    return ops.Tensor

  @property
  def output_shapes(self):
    return tensor_shape.TensorShape([])

  @property
  def output_types(self):
    return dtypes.string 
