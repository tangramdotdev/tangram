use ../../util/test.nu *

# Groups and organizations can be remote principals, and process tokens use the sandbox owner's remotes.

let remote_team = server spawn --name remote-team
let remote_org = server spawn --name remote-org
let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
tg --token $bob.token remote put --principal team shared $remote_team.url

let team_remotes = tg --token $bob.token remote list --principal team --expand | from json | get data
assert equal ($team_remotes | get name) [shared]
assert equal ($team_remotes | get url) [$remote_team.url]

let team_remote_get = tg --token $bob.token remote get --principal team shared | from json
assert equal $team_remote_get.url $remote_team.url
let team_remote_by_id = tg --token $bob.token remote get $team_remote_get.id | from json
assert equal $team_remote_by_id $team_remote_get "a remote should be found by its ID without its principal"
failure (tg --token $eve.token remote get $team_remote_get.id | complete) "Eve must not get a group-principal remote by its ID"
failure (tg --token $eve.token remote delete $team_remote_get.id | complete) "Eve must not delete a group-principal remote by its ID"

failure (tg --token $eve.token remote list --principal team | complete) "Eve must not list a group-principal remote without write on the group"

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $bob.token remote put --principal acme shared $remote_org.url

let org_remote_get = tg --token $bob.token remote get --principal acme shared | from json
assert equal $org_remote_get.url $remote_org.url

let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log(tg.process.env.TANGRAM_TOKEN);
			await tg.sleep(60);
		}
	'
}
let parent = tg --token $alice.token run --network=true --detach --verbose --owner team $path | from json
timeout 30s tg --token $alice.token log --no-timeout --length 1 $parent.process | ignore
let token = tg --token $alice.token log $parent.process | str trim

let process_remotes = tg --token $token remote list --expand | from json | get data
assert equal ($process_remotes | get name) [shared]
assert equal ($process_remotes | get url) [$remote_team.url]

tg --token $alice.token cancel $parent.process $parent.lease
tg --token $alice.token wait $parent.process
