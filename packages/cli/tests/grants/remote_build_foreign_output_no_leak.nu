use ../../test.nu *

# A remote build that names a foreign private object as its output must not grant the submitter access to it: building --remote runs on the remote but does not let the submitter read an object she could not read before.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }
let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

# Alice stores a private object on the remote.
let secret = tg --url $remote.url --token $alice.token put 'tg.file("alicesecret")' | str trim
tg --url $remote.url index

# Eve cannot read it.
let before = tg --url $remote.url --token $eve.token get $secret | complete
failure $before "Eve should not read Alice's private object before the build."

# Eve's local server, talking to the remote as Eve.
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve submits a remote build whose output references Alice's private object, and lets it run to a terminal state.
let src = 'export default function () { return tg.File.withId("SECRET"); }' | str replace "SECRET" $secret
let path = artifact { tangram.ts: $src }
let proc = tg --url $local.url build --remote --detach $path | str trim
wait_until { ((tg --url $remote.url --token $eve.token process status $proc | from json | get 0) in ["finished" "failed" "canceled"]) } --timeout 60sec

# Eve must still not read Alice's object: running it remotely does not launder access through the build's output.
let after = tg --url $remote.url --token $eve.token get $secret | complete
failure $after "a remote build must not grant the submitter access to a foreign object it names as output."
