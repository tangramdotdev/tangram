use ../lib/test.nu *

# A piped stdout response published while the response wait times out is retained
# rather than being lost in a subscription gap.

let server = server spawn --name server --config {
	advanced: {
		checkpoints: true,
	},
	roles: [api indexer scheduler],
}
let created = tg --url $server.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $server.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(0.05);
			console.log("line");
		}
	',
}

let publish_watch = (
	tg --url $server.url checkpoint watch process.control.response.publish --params '{"kind":"read"}'
	| from json
	| get watch
)
let published_watch = (
	tg --url $server.url checkpoint watch process.control.response.published --params '{"kind":"read"}'
	| from json
	| get watch
)
let timeout_watch = (
	tg --url $server.url checkpoint watch control.request.timeout
	| from json
	| get watch
)
let run = job spawn {
	let job_id = job id
	let output = tg --url $server.url run --sandbox $path | complete
	$output | job send --tag $job_id 0
}

# Publish the read response while the requester is held after the timeout.
tg --url $server.url checkpoint wait control.request.timeout $timeout_watch 0 | ignore
tg --url $server.url checkpoint wait process.control.response.publish $publish_watch 0 | ignore
tg --url $server.url checkpoint continue process.control.response.publish $publish_watch 0
tg --url $server.url checkpoint wait process.control.response.published $published_watch 0 | ignore
tg --url $server.url checkpoint continue process.control.response.published $published_watch 0
tg --url $server.url checkpoint continue control.request.timeout $timeout_watch 0
tg --url $server.url checkpoint unwatch control.request.timeout $timeout_watch
tg --url $server.url checkpoint unwatch process.control.response.publish $publish_watch
tg --url $server.url checkpoint unwatch process.control.response.published $published_watch

let output = job recv --tag $run --timeout 10sec
success $output
assert ($output.stdout | str contains "line")
