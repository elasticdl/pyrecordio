package(default_visibility = ["//recordio:__subpackages__"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_binary(
    name = "python/librecordio_dataset_op.so",
    linkshared = 1,
    srcs = [
        "cc/recordio_dataset_op.cc",
        "cc/recordio_chunk_parser.cc",
        "cc/recordio_chunk.cc",
        "cc/recordio_reader.cc",
        "cc/recordio_chunk_parser.h",
        "cc/recordio_chunk.h",
        "cc/recordio_reader.h",
        ],
)

py_library(
    name = "tf_recordio_dataset_lib",
    srcs = ["python/tf_recordio_dataset.py"],
    data = [
        ":python/librecordio_dataset_op.so",
    ],
)

py_test(
    name = "tf_recordio_dataset_test",
    srcs = ["python/tf_recordio_dataset_test.py"],
    deps = [
        ":tf_recordio_dataset_lib",
    ],
    data = [
        "python/test_data/test_data_with_snappy_compression",
    ],
)

py_library(
    name = "lib",
    srcs = [
        "__init__.py", 
        "python/__init__.py",
    ],
    deps = [":tf_recordio_dataset_lib",],
)
