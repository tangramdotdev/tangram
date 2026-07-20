use ../../test.nu *

# A build whose sandbox is owned by a group must run, not fail to read its command.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
tg --token $alice.token group create team

let path = artifact { tangram.ts: 'export default () => tg.file("x")' }
let process = tg --token $alice.token build --detach --owner team $path | str trim
tg --token $alice.token wait $process | complete | ignore
let data = tg --token $alice.token get $process | from json
assert ($data.error? == null) "a group-owned build must not fail to access its command"
