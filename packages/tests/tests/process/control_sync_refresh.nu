use ../../util/test.nu *

# A remote process that outlives the sync token pushes its outcome with a refreshed token.

let now = date now | date to-timezone UTC | format date '%Y-%m-%dT%H:%M:%SZ'
let root_token = random chars
let remote = server spawn --name remote --now $now --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	sync: { permission_time_to_live: 2 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --now $now --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}

let path = artifact {
	tangram.ts: 'export default async () => tg.file("refreshed");',
}

# Hold the process before it finishes and advance the clocks past the sync token's time to live.
let watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let build = job spawn {
	let id = job id
	timeout 60s tg --url $remote.url --token $root_token build $path | complete | job send --tag $id 0
}
tg --url $runner.url checkpoint wait runner.process.finish $watch 0 | ignore
advance_time $remote 10sec
advance_time $runner 10sec
tg --url $runner.url checkpoint continue runner.process.finish $watch 0
tg --url $runner.url checkpoint unwatch runner.process.finish $watch

# The outcome is pushed to the remote.
let output = job recv --tag $build --timeout 60sec
success $output
let id = $output.stdout | str trim
let contents = tg --url $remote.url --token $root_token read $id | complete
success $contents "the outcome must be pushed to the remote"
assert equal $contents.stdout "refreshed"
