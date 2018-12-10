# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_binary(
    name = "librecordio_dataset_op.so",
    linkshared = 1,
    srcs = ["recordio_dataset_op.cc"],
    deps = ["//data/tensorflow/recordio:reader"],
)

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
        ":librecordio_dataset_op.so",
        "test_data/test_data_with_snappy_compression",
    ],
)
