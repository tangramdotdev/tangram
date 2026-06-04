use ../../test.nu *

let remote = spawn --config { authentication: true } -n remote
let local = spawn --config { remotes: { default: { url: $remote.url } } } -n local

let login = tg login alice --remote | from json
assert equal $login.specifier alice
assert (not ($env.TANGRAM_CONFIG | path exists)) "The login should not write a local token."
