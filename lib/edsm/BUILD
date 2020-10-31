load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@raxxla_py_deps//:requirements.bzl", "requirement")
load("@io_bazel_rules_docker//python3:image.bzl", "py3_image")
load(
  "@io_bazel_rules_docker//container:container.bzl",
  "container_push",
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

py_binary(
  name = "pubsub_loader",
  srcs = ["pubsub_loader.py"],
  deps = [
    requirement("absl-py"),
    "@commonlib//google:bigquery",
    "@commonlib//google:gcs",
    ":utils",
  ],
)

py3_image(
  name = "pubsub_loader_image",
  srcs = ["pubsub_loader.py"],
  main = "pubsub_loader.py",
  deps = [
    requirement("absl-py"),
    "@commonlib//google:bigquery",
    "@commonlib//google:gcs",
    ":utils",
  ],
)

container_push(
   name = "pubsub_loader_image_push",
   image = ":pubsub_loader_image",
   format = "Docker",
   registry = "gcr.io",
   repository = "raxxla/edsm_loader",
   tag = "dev",
)
