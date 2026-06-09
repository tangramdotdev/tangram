use ../../test.nu *

# A remote declared in the config is listed without any put.

let server = spawn --config { remotes: { seeded: { url: "http://localhost:9999" } } }

let list = tg remote list | from json
assert equal $list [{ name: "seeded", url: "http://localhost:9999" }] "the config remote should be listed"
