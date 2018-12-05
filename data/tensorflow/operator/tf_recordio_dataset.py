import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import tensor_shape

recordio = tf.load_op_library('./librecordio_dataset_op.so')

class RecordIODataset(tf.data.Dataset):
  """A `Dataset` comprising records from one or more RecordIO files."""

  def __init__(self, filenames, start_chunk=1, chunk_count=0):
    """Creates a `RecordIODataset`.

    Args:
      filenames: A `tf.string` tensor containing one or more filenames.
      compression_type: (Optional.) A `tf.string` scalar evaluating to one of
        `""` (no compression), `"ZLIB"`, or `"GZIP"`.
      start_chunk: (Optional.) A `tf.int64` scalar representing the number of
        bytes in the read buffer. 0 means no buffering.
    """
    super(RecordIODataset, self).__init__()
    # Force the type to string even if filenames is an empty list.
    self._filenames = filenames
    self._start_chunk = start_chunk 
    self._chunk_count = chunk_count

  def _as_variant_tensor(self):
    return recordio.recordio_dataset(self._filenames, self._start_chunk, self._chunk_count)

  @property
  def output_classes(self):
    return ops.Tensor

  @property
  def output_shapes(self):
    return tensor_shape.TensorShape([])

  @property
  def output_types(self):
    return dtypes.string 
