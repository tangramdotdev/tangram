use ../../test.nu *

# A process authenticated client lists its creator's remotes but may not put a remote.

let root_remote = spawn --name root-remote
let alice_remote = spawn --name alice-remote
let server = spawn --config {
	authentication: true,
	remotes: { root: { url: $root_remote.url } },
}

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
tg --token $alice remote put alice $alice_remote.url

# Run a command that logs its process token and stays alive.
let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log(tg.process.env.TANGRAM_TOKEN);
			await tg.sleep(60);
		}
	'
}
let parent = tg build --detach --verbose $path | from json
wait_until { (tg log $parent.process | str trim | str length) > 0 } "the process should log its token"
let token = tg log $parent.process | str trim

# Listing remotes with a process token uses the creator's remotes.
let remotes = tg --token $token remote list | from json
assert equal ($remotes | get name) [alice]
assert equal ($remotes | get url) [$alice_remote.url]

# Getting a remote with a process token uses the creator's remotes.
let remote = tg --token $token remote get alice | from json
assert equal $remote.url $alice_remote.url

# Putting a remote with a process token is unauthorized.
let output = tg --token $token remote put upstream "http://localhost:9999" | complete
failure $output
assert ($output.stderr | str contains 'unauthorized') "the put should be unauthorized"

tg cancel $parent.process $parent.lease
tg wait $parent.process
