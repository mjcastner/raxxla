load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc//python:defs.bzl", "python_proto_library", "python_grpc_library")

python_grpc_library(
    name = "api_raxxla_grpc_py",
    deps = [":api_raxxla_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "api_raxxla_proto",
    srcs = ["api_raxxla.proto"],
    deps = [
        ":bodies_proto",
        ":settlement_proto",
        ":society_proto",
        ":system_proto",
    ],
)

proto_library(
    name = "bodies_proto",
    srcs = ["bodies.proto"],
)

proto_library(
    name = "poi_proto",
    srcs = ["poi.proto"],
    deps = [":system_proto"],
)

proto_library(
    name = "settlement_proto",
    srcs = ["settlement.proto"],
)

proto_library(
    name = "society_proto",
    srcs = ["society.proto"],
)

proto_library(
    name = "system_proto",
    srcs = ["system.proto"],
)

python_proto_library(
    name = "bodies_proto_py",
    deps = [":bodies_proto"],
    visibility = ["//visibility:public"],
)

python_proto_library(
    name = "poi_proto_py",
    deps = [":poi_proto"],
    visibility = ["//visibility:public"],
)

python_proto_library(
    name = "settlement_proto_py",
    deps = [":settlement_proto"],
    visibility = ["//visibility:public"],
)

python_proto_library(
    name = "society_proto_py",
    deps = [":society_proto"],
    visibility = ["//visibility:public"],
)

python_proto_library(
    name = "system_proto_py",
    deps = [":system_proto"],
    visibility = ["//visibility:public"],
)