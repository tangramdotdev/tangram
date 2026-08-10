use ../../test.nu *

# A watched checkin is scoped to its principal, so one user cannot reuse another user's authorized dependency solutions.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

# Alice creates a private tagged dependency in her namespace.
let secret_path = artifact {
	tangram.ts: '// alice secret'
}
tg --token $alice.token group create alice/secret | ignore
tg --token $alice.token tag alice/secret/1.0.0 $secret_path

let path = artifact {
	tangram.ts: 'import secret from "alice/secret/*";'
}

# Alice can solve the private dependency and establish a watch.
let alice_checkin = tg --token $alice.token checkin $path --watch --no-cache-pointers --no-lock | complete
success $alice_checkin

# Bob cannot cold-check in the source because he cannot access Alice's dependency.
let bob_cold_checkin = tg --token $bob.token checkin $path --no-cache-pointers --no-lock | complete
failure $bob_cold_checkin

# Bob must not reuse Alice's authorized watch state.
let bob_watched_checkin = tg --token $bob.token checkin $path --watch --no-cache-pointers --no-lock | complete
failure $bob_watched_checkin
