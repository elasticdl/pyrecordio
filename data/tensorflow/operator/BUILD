package(default_visibility = ["//visibility:public"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_binary(
    name = "librecordio_dataset_op.so",
    linkshared = 1,
    srcs = ["recordio_dataset_op.cc"],
    deps = ["//data/tensorflow/recordio:reader"],
)
