package(default_visibility = ["//visibility:public"])

py_library(
    name = "lib",
    srcs = [
        "__init__.py",
        "chunk.py",
        "file_index.py",
        "file.py",
        "global_variables.py",
        "header.py",
        "reader.py",
        "writer.py",
    ]
)

py_test(
    name = "chunk_test",
    srcs = ["chunk_test.py"],
    deps = [":lib"],
)

py_test(
    name = "file_index_test",
    srcs = ["file_index_test.py"],
    deps = [":lib"],
)

py_test(
    name = "file_test",
    srcs = ["file_test.py"],
    deps = [":lib"],
)

py_test(
    name = "header_test",
    srcs = ["header_test.py"],
    deps = [":lib"],
)

py_test(
    name = "writer_reader_test",
    srcs = ["writer_reader_test.py"],
    deps = [":lib"],
)
