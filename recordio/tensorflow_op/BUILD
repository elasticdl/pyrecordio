package(default_visibility = ["//recordio:__subpackages__"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_binary(
    name = "python/librecordio_dataset_op.so",
    srcs = [
        "cc/recordio_chunk.cc",
        "cc/recordio_chunk.h",
        "cc/recordio_chunk_parser.cc",
        "cc/recordio_chunk_parser.h",
        "cc/recordio_dataset_op.cc",
        "cc/recordio_reader.cc",
        "cc/recordio_reader.h",
    ],
    copts = [
        "-pthread",
        "-std=c++11",
        "-D_GLIBCXX_USE_CXX11_ABI=0",
    ],
    linkshared = 1,
    deps = [
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
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
    data = [
        "python/test_data/test_data_with_snappy_compression",
    ],
    deps = [
        ":tf_recordio_dataset_lib",
    ],
)

py_library(
    name = "lib",
    srcs = [
        "__init__.py",
        "python/__init__.py",
    ],
    deps = [":tf_recordio_dataset_lib"],
)
