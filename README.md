# RecordIO

RecordIO is a file format created for [PaddlePaddle Elastic Deep Learning](https://kubernetes.io/blog/2017/12/paddle-paddle-fluid-elastic-learning/).  It is generally useful for distributed computing.

## Motivations

In distributed computing, we often need to dispatch tasks to worker processes.  Usually, a task is defined as a partition of the input data, like what MapReduce and distributed machine learning do.

Most distributed filesystems, including HDFS, Google FS, and CephFS, prefer a small number of big files.  Therefore, it is impractical to create each task as a small file; instead, we need a format for big files that is

1. appendable, so that applications can append records to the file without updating the meta-data, thus fault tolerable,
2. partitionable, so that applications can quickly scan over the file to count the total number of records, and create tasks each corresponds to a sequence of records.

RecordIO is such a file format.

## Write 

```python
import recordio

# write
with recordio.File('demo.recordio', 'w') as rdio_w: 
    rdio_w.write('abc')
    rdio_w.write('def')
```

## Read

```python
with recordio.File('demo.recordio', 'r') as rdio_r:
    # Random access
    for i in range(rdio_r.count()):
        print(rdio_r.get(i))

    # Range reading
    for record in rdio_r.get_reader(2, 10):
        print(record)

    # Direct iteration
    for record in rdio_r:
        print(record)
```

## Unittest

In this directory:

```bash
python -m unittest recordio/recordio/*_test.py
```

## Packaging

The package process largely follows the example of [Tensorflow custom op](https://github.com/tensorflow/custom-op)

First build RecordIO devel Docker image:
```bash
docker build -t recordio:dev -f Dockerfile .
```

Start Docker container and map `git` and bazel `.cache` directories:
```bash
docker run --rm -it \
    -v $HOME/git:/git \
    -v $HOME/.cache:/.cache \
    -w /git/pyrecordio \
    recordio:dev
```

Inside container, build the pip package:
```bash
bazel build build_pip_pkg 
bazel-bin/build_pip_pkg artifacts
```

After building the package, force install it to replace the existing version.
```bash
pip install -I artifacts/recordio-<version>.whl
```

To test the installed TensorFlow RecordIO Dataset op:
```bash
python recordio/tensorflow_op/python/tf_recordio_dataset_test.py
```

There is also a prepackded version checked into the repo for convenience.
