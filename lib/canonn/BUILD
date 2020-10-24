load("@rules_python//python:defs.bzl", "py_binary")
load("@raxxla_deps//:requirements.bzl", "requirement")

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  python_version = "PY3",
  deps = [
    requirement("absl-py"),
    "@commonlib//google:gcs",
  ],
)
