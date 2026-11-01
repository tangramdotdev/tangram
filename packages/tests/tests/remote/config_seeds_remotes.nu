use ../../util/test.nu *

# A remote declared in the config is listed without any put.

let local = server spawn --config { remotes: { seeded: { url: "http://localhost:9999" } } }

let list = tg remote list --expand | from json | get data | reject id
assert equal $list [{ name: "seeded", trusted: false, url: "http://localhost:9999" }] "the config remote should be listed"
