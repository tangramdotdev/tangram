use ../lib/test.nu *

# The built-in Tangram Cloud remote is trusted.

let local = server spawn --config { remotes: null }
let remote = tg --url $local.url remote get default | from json

assert equal $remote.url "https://cloud.tangram.dev" "the built-in remote should target Tangram Cloud"
assert equal $remote.trusted true "the built-in Tangram Cloud remote should be trusted"
