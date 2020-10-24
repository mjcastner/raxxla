load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@raxxla_py_deps//:requirements.bzl", "requirement")

py_binary(
  name = "dep_test",
  srcs = ["dep_test.py"],
  deps = [
    requirement("absl-py"),
    "@commonlib//google:gcs",
    "//proto:bodies_py_proto",
    "//proto:settlement_py_proto",
    "//proto:society_py_proto",
    "//proto:system_py_proto",
  ],
)

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  deps = [
    requirement("absl-py"),
    "@commonlib//google:gcs",
    ":utils",
  ],
)

py_library(
  name = "utils",
  srcs = ["utils.py"],
  deps = [
    requirement("absl-py"),
    "//proto:bodies_py_proto",
    "//proto:settlement_py_proto",
    "//proto:society_py_proto",
    "//proto:system_py_proto",
  ],
)
