use ../../util/test.nu *

# A reference token remains valid across graceful and abrupt restarts and is rejected by a different server.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } }, vfs: false }
let alice = tg --url $local.url login --verbose --name alice | from json
let bob = tg --url $local.url login --verbose --name bob | from json
let group = tg --url $local.url --token $alice.token group create --verbose private | from json
let token = $group.tokens.local.0 | url encode --all
let reference = $"private?tokens[local][0]=($token)"
success (tg --url $local.url --token $bob.token get $reference | complete) 'the token must be accepted'

# The token is accepted after a graceful restart.
let local = server restart $local
success (tg --url $local.url --token $bob.token get $reference | complete) 'the token must be accepted after a graceful restart'

# The token is accepted after an abrupt restart.
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid 'the server must stop'
let local = server start $local
success (tg --url $local.url --token $bob.token get $reference | complete) 'the token must be accepted after an abrupt restart'
server stop $local

# A different server rejects the token.
let other = server spawn --config { authentication: { users: { providers: { insecure: true } } }, vfs: false }
let alice = tg --url $other.url login --verbose --name alice | from json
let bob = tg --url $other.url login --verbose --name bob | from json
tg --url $other.url --token $alice.token group create private | ignore
failure (tg --url $other.url --token $bob.token get private | complete) 'the group must be private'
failure (tg --url $other.url --token $bob.token get $reference | complete) 'a different server must reject the token'
