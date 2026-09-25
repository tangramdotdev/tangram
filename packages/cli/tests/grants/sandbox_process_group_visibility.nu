use ../lib/test.nu *

# Sandbox access does not confer process access; a process grant is required.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

# Bob builds a private process and another in a sandbox owned by the team.
let private_path = artifact { tangram.ts: 'export default function () { return tg.file("group-visibility-private"); }' }
let private = tg --token $bob.token build --detach $private_path | str trim
tg --token $bob.token wait $private | complete | ignore

let team_path = artifact { tangram.ts: 'export default function () { return tg.file("group-visibility-team"); }' }
let process = tg --token $bob.token build --detach --group team $team_path | str trim
tg --token $bob.token wait $process | complete | ignore

# Carol cannot read either process through her group membership.
failure (tg --token $carol.token get $process | complete) "sandbox access must not confer process access"
failure (tg --token $carol.token get $private | complete) "a group member must not read another member's private process"
let denied = tg --token $eve.token get $process | complete
failure $denied "an outsider must not read a process owned by a group"
snapshot --normalize-ids $denied.stderr '
	error an error occurred
	-> failed to get the process
	   id = pcs_0000000000000000000000000000
	-> failed to get the process

'

# An explicit process grant provides access independently of the sandbox.
tg --token $bob.token grant $carol.user.id process_subtree $process
success (tg --token $carol.token get $process | complete) "a process grant should allow reading the process"
