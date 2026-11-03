use ../../util/test.nu *

# The remotes key in the config is authoritative at startup: a restart restores a deleted config remote and removes a remote added through the CLI.

let config = { remotes: { seeded: { url: "http://localhost:9999" } } }
let local = server spawn --name local --config $config

tg remote delete seeded
tg remote put added "http://localhost:7777"
assert equal (tg remote list --expand | from json | get data.name) ["added"] "the session mutations should apply"

# Restart the server with the same config.
let server = server restart $local

let list = tg remote list --expand | from json | get data | reject id
assert equal $list [{ name: "seeded", trusted: false, url: "http://localhost:9999" }] "the config should restore its remotes and remove the added one"
