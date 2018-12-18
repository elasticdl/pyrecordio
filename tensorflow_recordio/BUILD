py_library(
    name = "tf_recordio_dataset",
    srcs = ["tf_recordio_dataset.py"],
)

py_test(
    name = "tf_recordio_dataset_test",
    srcs = ["tf_recordio_dataset_test.py"],
    deps = [
        ":tf_recordio_dataset",
    ],
    data = [
        "//data/tensorflow/operator:librecordio_dataset_op.so",
        "test_data/test_data_with_snappy_compression",
    ],
)
