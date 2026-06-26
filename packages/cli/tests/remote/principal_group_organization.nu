use ../../test.nu *

# Groups and organizations can be remote principals, and process tokens use the sandbox owner's remotes.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let team_remote = spawn --name team-remote
let org_remote = spawn --name org-remote

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
tg --token $bob.token remote put --principal team shared $team_remote.url

let team_remotes = tg --token $bob.token remote list --principal team | from json
assert equal ($team_remotes | get name) [shared]
assert equal ($team_remotes | get url) [$team_remote.url]

let team_remote_get = tg --token $bob.token remote get --principal team shared | from json
assert equal $team_remote_get.url $team_remote.url

failure (tg --token $eve.token remote list --principal team | complete) "Eve must not list a group-principal remote without write on the group"

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $bob.token remote put --principal acme shared $org_remote.url

let org_remote_get = tg --token $bob.token remote get --principal acme shared | from json
assert equal $org_remote_get.url $org_remote.url

let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log(tg.process.env.TANGRAM_TOKEN);
			await tg.sleep(60);
		}
	'
}
let parent = tg --token $alice.token run --network=true --detach --verbose --owner team $path | from json
wait_until { (tg --token $alice.token log $parent.process | str trim | str length) > 0 } "the process should log its token"
let token = tg --token $alice.token log $parent.process | str trim

let process_remotes = tg --token $token remote list | from json
assert equal ($process_remotes | get name) [shared]
assert equal ($process_remotes | get url) [$team_remote.url]

tg --token $alice.token cancel $parent.process $parent.lease
tg --token $alice.token wait $parent.process
