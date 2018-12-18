# RecordIO

RecordIO is a file format created for [PaddlePaddle Elastic Deep Learning](https://kubernetes.io/blog/2017/12/paddle-paddle-fluid-elastic-learning/).  It is generally useful for distributed computing.

## Motivations

In distributed computing, we often need to dispatch tasks to worker processes.  Usually, a task is defined as a parition of the input data, like what MapReduce and distributed machine learning do.

Most distributed filesystems, including HDFS, Google FS, and CephFS, prefer a small number of big files.  Therefore, it is impratical to create each task as a small file; instead, we need a format for big files that is

1. appenable, so that applications can append records to the file without updating the meta-data, thus fault tolerable,
2. partitionable, so that applications can quickly scan over the file to count the total number of records, and create tasks each corresponds to a sequence of records.

RecordIO is such a file format.

## Write 

```python
import recordio

data = open('demo.recordio', 'wb')
max_chunk_size = 1024
writer = recordio.Writer(data, max_chunk_size)
writer.write('abc')
writer.write('edf')
writer.flush()
data.close()
```

## Read

```python
import recordio

data = open('demo.recordio', 'rb')   
index = recordio.FileIndex(data)
print('Total file records: ' + str(index.total_records()))

for i in range(index.total_chunks()):
  reader = recordio.Reader(data, index.chunk_offset(i))
  print('Total chunk records: ' + str(reader.total_count()))

  while reader.has_next():
    print('record value: ' + reader.next())

data.close()
```

## RecordIO File
```python
import recordio

# write
rdio_w = recordio.File('demo.recordio', 'w')
rdio_w.write('abc')
rdio_w.write('def')
rdio_w.close()

# read
rdio_r = recordio.File('demo.recordio', 'r')
iterator = rdio_r.iterator()       
while iterator.has_next():
    record = iterator.next()
rdio_r.close()
```

## Packageing
The package process largely follows the example of [Tensorflow custom op](https://github.com/tensorflow/custom-op)

```bash
bazel build build_pip_package
bazel-bin/build_pip_pkg artifacts
```
