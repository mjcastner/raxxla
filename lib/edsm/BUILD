load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@raxxla_py_deps//:requirements.bzl", "requirement")

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  deps = [
    requirement("absl-py"),
    "@commonlib//google:gcs",
    "@commonlib//google:pubsub",
    ":utils",
  ],
)

py_library(
  name = "utils",
  srcs = ["utils.py"],
  deps = [
    requirement("absl-py"),
    "//lib/proto:bodies_py_proto",
    "//lib/proto:settlement_py_proto",
    "//lib/proto:society_py_proto",
    "//lib/proto:system_py_proto",
  ],
)
