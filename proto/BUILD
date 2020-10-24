load("@rules_proto//proto:defs.bzl", "proto_library")
load("@build_stack_rules_proto//python:python_proto_library.bzl", "python_proto_library")

proto_library(
  name = "bodies_proto",
  srcs = ["bodies.proto"],
)

python_proto_library(
  name = "bodies_py_proto",
  deps = [":bodies_proto"],
  visibility = ["//visibility:public"],
)

proto_library(
  name = "poi_proto",
  srcs = ["poi.proto"],
  deps = [":system_proto"]
)

python_proto_library(
  name = "poi_py_proto",
  deps = [":poi_proto"],
  visibility = ["//visibility:public"],
)

proto_library(
  name = "settlement_proto",
  srcs = ["settlement.proto"],
)

python_proto_library(
  name = "settlement_py_proto",
  deps = [":settlement_proto"],
  visibility = ["//visibility:public"],
)

proto_library(
  name = "society_proto",
  srcs = ["society.proto"],
)

python_proto_library(
  name = "society_py_proto",
  deps = [":society_proto"],
  visibility = ["//visibility:public"],
)

proto_library(
  name = "system_proto",
  srcs = ["system.proto"],
)

python_proto_library(
  name = "system_py_proto",
  deps = [":system_proto"],
  visibility = ["//visibility:public"],
)
