use ../../test.nu *

let remote = spawn --config { authorization: true } -n remote
let local = spawn --config { remotes: [{ name: default, url: $remote.url }] } -n local

let login = tg login alice@example.com --handle alice | from json
assert equal $login.handle alice
assert equal $login.location remote
assert (not ($env.TANGRAM_CONFIG | path exists)) "The login should not write a local token."

let whoami = tg whoami | from json
assert equal $whoami.handle alice
assert equal $whoami.location remote
