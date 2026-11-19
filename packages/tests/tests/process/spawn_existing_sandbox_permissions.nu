use ../../util/test.nu *

# A user's process in an existing sandbox can be read, waited on, and canceled by the user and not read by another user, and a public process in the sandbox can be read by an anonymous client.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Run a command as an anonymous client.
let config = mktemp
{} | to json | save -f $config
let anonymous = {|command| with-env { TANGRAM_CONFIG: $config } { do $command | complete } }

# Alice spawns a private and a public process in her sandbox, without tokens, so that only their permissions authorize them.
let sandbox = tg --token $alice.token sandbox create --no-tokens | referent node
let private = tg --token $alice.token spawn --no-tokens --verbose $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'while :; do :; done' | from json
let public = tg --token $alice.token spawn --no-tokens --verbose --public $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'while :; do :; done' | from json
tg index

# Alice can read her private process, and Eve and an anonymous client cannot.
success (tg --token $alice.token process get $private.process | complete) 'Alice should read her process'
failure (tg --token $eve.token process get $private.process | complete) 'Eve must not read Alice process'
failure (do $anonymous { tg process get $private.process }) 'an anonymous client must not read Alice private process'

# An anonymous client can read the public process.
success (do $anonymous { tg process get $public.process }) 'an anonymous client should read the public process'

# Alice can cancel her processes and wait on them.
for spawned in [$private $public] {
	tg --token $alice.token cancel $spawned.process $spawned.lease
	let wait = tg --token $alice.token wait $spawned.process | complete
	success $wait 'Alice should wait on her process'
	assert equal ($wait.stdout | from json | get exit) 1 'the canceled process should fail'
}
failure (tg --token $eve.token wait $private.process | complete) 'Eve must not wait on Alice process'
tg --token $alice.token sandbox destroy $sandbox
