use ../../test.nu *

# The remotes key in the config is authoritative at startup: a restart restores a deleted config remote and removes a remote added through the CLI.

let config = { remotes: { seeded: { url: "http://localhost:9999" } } }
let server = server spawn --name seeded --config $config

tg remote delete seeded
tg remote put added "http://localhost:7777"
assert equal (tg remote list | from json | get name) ["added"] "the session mutations should apply"

# Restart the server with the same config.
let server = server restart $server

let list = tg remote list | from json
assert equal $list [{ name: "seeded", url: "http://localhost:9999" }] "the config should restore its remotes and remove the added one"
