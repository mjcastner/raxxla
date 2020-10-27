workspace(name = "raxxla")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Proto rules
git_repository(
    name = "build_stack_rules_proto",
    remote = "git@github.com:stackb/rules_proto.git",
    commit = "b2913e6340bcbffb46793045ecac928dcf1b34a5"
)
load("@build_stack_rules_proto//python:deps.bzl", "python_proto_library")
python_proto_library()

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()

# Commonlib setup
git_repository(
    name = "commonlib",
    remote = "git@github.com:mjcastner/commonlib.git",
    branch = "main",
)

# Python rules
http_archive(
    name = "rules_python",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.1.0/rules_python-0.1.0.tar.gz",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
)
load("@rules_python//python:repositories.bzl", "py_repositories")
py_repositories()

load("@rules_python//python:pip.bzl", "pip_install")
pip_install(
  name = "google_py_deps",
  requirements = "@commonlib//google:requirements.txt",
  python_interpreter_target = "@python_interpreter//:python_bin",
)

pip_install(
  name = "raxxla_py_deps",
  requirements = "//lib:requirements.txt",
  python_interpreter_target = "@python_interpreter//:python_bin",
)

pip_install(
    name = "protobuf_py_deps",
    requirements = "@build_stack_rules_proto//python/requirements:protobuf.txt",
    python_interpreter_target = "@python_interpreter//:python_bin",
)

_py_configure = """
if [[ "$OSTYPE" == "darwin"* ]]; then
   ./configure --prefix=$(pwd)/bazel_install --with-openssl=$(brew --prefix openssl)
else
   ./configure --prefix=$(pwd)/bazel_install
fi
"""

http_archive(
   name = "python_interpreter",
   build_file_content = """
exports_files(["python_bin"])
filegroup(
   name = "files",
   srcs = glob(["bazel_install/**"], exclude = ["**/* *"]),
   visibility = ["//visibility:public"],
)
""",
   patch_cmds = [
       "mkdir $(pwd)/bazel_install",
       _py_configure,
       "make",
       "make install",
       "ln -s bazel_install/bin/python3 python_bin",
   ],
   sha256 = "dfab5ec723c218082fe3d5d7ae17ecbdebffa9a1aea4d64aa3a2ecdd2e795864",
   strip_prefix = "Python-3.8.3",
   urls = ["https://www.python.org/ftp/python/3.8.3/Python-3.8.3.tar.xz"],
)
register_toolchains("//:my_py_toolchain")
