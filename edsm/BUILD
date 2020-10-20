load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@raxxla_deps//:requirements.bzl", "requirement")

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  python_version = "PY3",
  deps = [
    requirement("absl-py"),
    ":utils",
    "@commonlib//google:gcs",
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
