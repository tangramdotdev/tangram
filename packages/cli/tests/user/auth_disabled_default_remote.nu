use ../../test.nu *

# Logging in with the remote flag against a local server with a default remote authenticates on the remote without writing a local token.

let remote = spawn --config { authentication: true } --name remote
let local = spawn --config { remotes: { default: { url: $remote.url } } } --name local

let login = tg login alice --remote | from json
assert equal $login.specifier alice
assert (not ($env.TANGRAM_CONFIG | path exists)) "The login should not write a local token."
