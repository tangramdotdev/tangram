use ../../util/test.nu *
use ../../util/nats.nu *

# Bulk invalidation publication does not delay the response after its database commit.
let nats = spawn_nats
let root = random chars
let local = server spawn --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } },
	http: { request_timeout: 2 },
	messenger: { kind: nats, url: $nats.url },
}
let users = [alice bob carol] | each { |name| tg --token $root login --verbose --name $name | from json }
let target = tg --token $root put --no-tokens 'tg.file("replacement")' | referent node
stop_nats $nats
let socket = $local.url | str replace 'http+unix://' '' | url decode
let tags = $users | each { |user| { specifier: $user.user.specifier, target: { kind: object, id: $target } } }
{ force: true, tags: $tags }
| http post --content-type application/json --headers { Authorization: $'Bearer ($root)' } --unix-socket $socket 'http://localhost/tags/batch'
| ignore
for user in $users {
	failure (tg --token $user.token remote list | complete) 'the committed bulk replacement should revoke every user token'
}
