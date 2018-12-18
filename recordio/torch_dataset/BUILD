package(default_visibility = ["//visibility:public"])

py_library(
    name = "lib",
    srcs = ["torch_dataset.py"],
    deps = ["//recordio/recordio:lib"]
)

py_test(
    name = "torch_dataset_test",
    srcs = ["torch_dataset_test.py"],
    deps = [":lib"],
)
