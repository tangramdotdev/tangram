use ../../test.nu *

# A process owned by a group is readable by group members and hidden from outsiders, because reading a process requires reading its sandbox.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

# Bob builds a private process for himself and another whose sandbox is owned by the team. Regardless of whether the build succeeds, the process record's visibility follows its sandbox owner.
let private_path = artifact { tangram.ts: 'export default function () { return tg.file("group-visibility-private"); }' }
let private = tg --token $bob.token build --detach $private_path | str trim
tg --token $bob.token wait $private | complete | ignore

let team_path = artifact { tangram.ts: 'export default function () { return tg.file("group-visibility-team"); }' }
let process = tg --token $bob.token build --detach --group team $team_path | str trim
tg --token $bob.token wait $process | complete | ignore

# Carol, a member, can read the team-owned process but not Bob's private process; Eve, an outsider, can read neither.
success (tg --token $carol.token get $process | complete) "a group member should read a process owned by the group"
failure (tg --token $carol.token get $private | complete) "a group member must not read another member's private process"
let denied = tg --token $eve.token get $process | complete
failure $denied "an outsider must not read a process owned by a group"
snapshot ($denied.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to find the process
	   id = <process>

'
