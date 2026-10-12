use ../lib/test.nu *

# A remote process that outlives the sync token pushes its outcome with a refreshed token.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	sync: { permission_time_to_live: 2 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}

let path = artifact {
	tangram.ts: 'export default async () => { await tg.sleep(6); return tg.file("refreshed"); };',
}
let output = timeout 60s tg --url $remote.url --token $root_token build $path | complete
success $output
let id = $output.stdout | str trim
let contents = tg --url $remote.url --token $root_token read $id | complete
success $contents "the outcome must be pushed to the remote"
assert equal $contents.stdout "refreshed"
