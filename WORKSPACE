load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Protobuf rules
git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf",
    tag = "v3.10.0",
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

# Python rules
http_archive(
    name = "rules_python",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
)
load("@rules_python//python:pip.bzl", "pip_install")

# Commonlib setup
git_repository(
    name = "commonlib",
    remote = "git@github.com:mjcastner/commonlib.git",
    branch = "main",
)
pip_install(
   name = "google_deps",
   requirements = "@commonlib//google:requirements.txt",
   python_interpreter = "python3",
)

# Raxxla setup
pip_install(
   name = "raxxla_deps",
   requirements = "//lib:requirements.txt",
   python_interpreter = "python3",
)
