use ../../test.nu *

# Spawning a process into an existing sandbox requires write on that sandbox, so a user who cannot write it is denied while the owner can.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let sandbox = tg --token $alice.token sandbox create --no-network | str trim
let path = artifact { tangram.ts: 'export default () => tg.file("spawn-into-existing")' }

# Eve cannot write Alice's sandbox, so she must not spawn a process into it.
failure (tg --token $eve.token run $"--sandbox=($sandbox)" $path | complete) "Eve must not spawn a process into a sandbox she cannot write"

# Alice owns the sandbox, so she can spawn a process into it.
success (tg --token $alice.token run $"--sandbox=($sandbox)" $path | complete) "Alice should spawn a process into her own sandbox"
