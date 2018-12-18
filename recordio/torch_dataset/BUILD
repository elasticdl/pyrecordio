package(default_visibility = ["//visibility:public"])

py_library(
    name = "torch_dataset",
    srcs = ["torch_dataset.py"],
    deps = ["//recordio/recordio:lib"]
)

py_test(
    name = "torch_dataset_test",
    srcs = ["torch_dataset_test.py"],
    deps = [":torch_dataset"],
)
