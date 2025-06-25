load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name  = "kvs_tests",
    srcs  = glob(["*.rs"]),       # all your test files
    deps  = ["//src:rust_kvs"],   # link in your library under src/
    tags  = ["no_rustfmt", "no_clippy"],  # if you still have those aspects enabled
)
