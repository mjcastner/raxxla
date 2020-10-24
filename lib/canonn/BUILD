load("@rules_python//python:defs.bzl", "py_binary")
load("@raxxla_py_deps//:requirements.bzl", "requirement")

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  deps = [
    requirement("absl-py"),
    "@commonlib//google:sheets",
    "//proto:poi_py_proto",
  ],
)
