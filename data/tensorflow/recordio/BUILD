package(default_visibility = ["//data:__subpackages__"])

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "chunk",
    srcs = ["recordio_chunk.cc"],
    hdrs = ["recordio_chunk.h"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "chunk_parser",
    srcs = ["recordio_chunk_parser.cc"],
    hdrs = ["recordio_chunk_parser.h"],
    deps = [":chunk"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "index",
    srcs = ["recordio_index.cc"],
    hdrs = ["recordio_index.h"],
    deps = [":chunk"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "reader",
    srcs = ["recordio_reader.cc"],
    hdrs = ["recordio_reader.h"],
    deps = [
        ":chunk_parser",
        ":index",
    ],
)
