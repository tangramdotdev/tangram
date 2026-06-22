use ../../test.nu *

# A process's log is masked from a principal without a grant: the owner reads it, but knowing the process id is not enough for another principal.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process that logs a secret.
let path = artifact { tangram.ts: 'export default async () => { console.log("topsecret"); return 0 }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process

# The owner can read the log.
let owner = tg --token $alice.token log $process | complete
snapshot ($owner.stdout | redact) '
	topsecret

'

# Eve cannot read Alice's private process.
let denied = tg --token $eve.token get $process | complete
failure $denied "Eve should not read Alice's private process."

# Eve must not read the process's log either.
let leaked = tg --token $eve.token log $process | complete
snapshot ($leaked.stdout | redact) ''
