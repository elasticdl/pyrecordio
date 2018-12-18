package(default_visibility = ["//visibility:public"])

py_library(
    name = "lib",
    srcs = ["tf_recordio_dataset.py"],
    data = [
        "//recordio/tensorflow_op/cc:librecordio_dataset_op.so",
    ],
)

py_test(
    name = "tf_recordio_dataset_test",
    srcs = ["tf_recordio_dataset_test.py"],
    deps = [
        ":lib",
    ],
    data = [
        "test_data/test_data_with_snappy_compression",
    ],
)
