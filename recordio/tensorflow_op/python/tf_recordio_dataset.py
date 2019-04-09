import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import load_library
from tensorflow.python.framework import tensor_shape
from tensorflow.python.platform import resource_loader
import os.path

# bazel's runfile tree is a mess. https://github.com/bazelbuild/bazel/issues/2339
# Here we do some hack to find the dynamic library in different cases. Use the 
# one we found last.
__lib_paths = [
    # If the pip library is installed
    resource_loader.get_path_to_datafile('librecordio_dataset_op.so'),
    # For bazel_test in this WORKSPACE
    'recordio/tensorflow_op/python/librecordio_dataset_op.so',
]

__fp = None
for fp in __lib_paths:
    found = os.path.isfile(fp)
    print(fp, "FOUND" if found else "NOT FOUND")
    if found: __fp = fp

recordio = load_library.load_op_library(__fp)

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
