load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@raxxla_deps//:requirements.bzl", raxxla_requirement = "requirement")

py_binary(
    name = "pubsub_loader",
    srcs = ["pubsub_loader.py"],
    python_version = "PY3",
    deps = [
        raxxla_requirement("absl-py"),
        ":utils",
        "@commonlib//google:gcs",
    ],
)

py_library(
    name = "utils",
    srcs = ["utils.py"],
    deps = [
        raxxla_requirement("absl-py"),
        "//proto:raxxla_proto",
    ],
)
