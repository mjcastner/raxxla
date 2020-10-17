load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf",
    tag = "v3.10.0",
)

git_repository(
    name = "commonlib",
    remote = "git@github.com:mjcastner/commonlib.git",
    branch = "main",
)

http_archive(
    name = "rules_python",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.0.2/rules_python-0.0.2.tar.gz",
    strip_prefix = "rules_python-0.0.2",
    sha256 = "b5668cde8bb6e3515057ef465a35ad712214962f0b3a314e551204266c7be90c",
)

load("@rules_python//python:pip.bzl", "pip3_import")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

pip3_import(
   name = "raxxla_deps",
   requirements = "requirements.txt",
)
load("@raxxla_deps//:requirements.bzl", "pip_install")
pip_install()

pip3_import(
   name = "google_deps",
   requirements = "@commonlib//:external/google_requirements.txt",
)
load("@google_deps//:requirements.bzl", "pip_install")
pip_install()
