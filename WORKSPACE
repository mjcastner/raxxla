workspace(name = "raxxla")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Special logic for building python interpreter with OpenSSL from homebrew.
# See https://devguide.python.org/setup/#macos-and-os-x
_py_configure = """
if [[ "$OSTYPE" == "darwin"* ]]; then
    ./configure --prefix=$(pwd)/bazel_install --with-openssl=$(brew --prefix openssl)
else
    ./configure --prefix=$(pwd)/bazel_install
fi
"""

http_archive(
    name = "python_interpreter",
    # urls = ["https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tar.xz"],
    # sha256 = "0382996d1ee6aafe59763426cf0139ffebe36984474d0ec4126dd1c40a8b3549",
    # strip_prefix = "Python-3.7.0",
    # urls = ["https://www.python.org/ftp/python/3.8.3/Python-3.8.3.tar.xz"],
    # sha256 = "dfab5ec723c218082fe3d5d7ae17ecbdebffa9a1aea4d64aa3a2ecdd2e795864",
    # strip_prefix = "Python-3.8.3",
    urls = ["https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tar.xz"],
    sha256 = "9c73e63c99855709b9be0b3cc9e5b072cb60f37311e8c4e50f15576a0bf82854",
    strip_prefix = "Python-3.9.0",
    patch_cmds = [
        "mkdir $(pwd)/bazel_install",
        _py_configure,
        "make",
        "make install",
        "ln -s bazel_install/bin/python3 python_bin",
    ],
    build_file_content = """
exports_files(["python_bin"])
filegroup(
    name = "files",
    srcs = glob(["bazel_install/**"], exclude = ["**/* *"]),
    visibility = ["//visibility:public"],
)
""",
)

http_archive(
    name = "rules_python",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
)

http_archive(
    name = "rules_proto_grpc",
    sha256 = "d771584bbff98698e7cb3cb31c132ee206a972569f4dc8b65acbdd934d156b33",
    strip_prefix = "rules_proto_grpc-2.0.0",
    urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/archive/2.0.0.tar.gz"],
)

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")
rules_proto_grpc_toolchains()
rules_proto_grpc_repos()
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()
load("@rules_proto_grpc//python:repositories.bzl", rules_proto_grpc_python_repos = "python_repos")
rules_proto_grpc_python_repos()
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()

# Built-in PIP
load("@rules_python//python:pip.bzl", "pip_install")
pip_install(
    name = "raxxla_deps",
    requirements = "//backend:requirements.txt",
    python_interpreter_target = "@python_interpreter//:python_bin",
    extra_pip_args = ["--platform", "linux_x86_64"],
)

register_toolchains("//:my_py_toolchain")