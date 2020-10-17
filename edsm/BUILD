load("@rules_python//python:defs.bzl", "py_binary")
load("@rules_python//python:defs.bzl", "py_library")
load("@raxxla_deps//:requirements.bzl", "requirement")

py_binary(
    name = "pubsub_loader",
    srcs = ["pubsub_loader.py"],
    python_version = "PY3",
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
    "//proto:raxxla_proto",
  ]
)
