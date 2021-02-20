load("@rules_python//python:defs.bzl", "py_binary")
load("@raxxla_deps//:requirements.bzl", "requirement")

py_binary(
    name = "main",
    srcs = ["main.py"],
    python_version = "PY3",
    deps = [
        "//commonlib/google/datastore:datastore",
        "//commonlib:utils",
        "//protos:api_raxxla_grpc_py",
        "//protos:bodies_proto_py",
        "//protos:settlement_proto_py",
        "//protos:society_proto_py",
        "//protos:system_proto_py",
        requirement("absl-py"),
        # requirement("cython"),
        requirement("google-cloud-datastore"),
        requirement("grpcio"),
        requirement("grpcio-tools"),
    ],
)