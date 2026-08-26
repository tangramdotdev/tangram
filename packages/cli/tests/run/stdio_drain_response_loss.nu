use ../../test.nu *

# A piped stdout response published while the response wait times out is retained
# rather than being lost in a subscription gap.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
	runner: {
		stdio_drain_timeout: 0.01,
	},
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn(child).stdio("pipe").sandbox();
			let stdout = await process.stdout.readAllToString();
			console.log(stdout);
		}

		export async function child() {
			await tg.sleep(0.05);
			console.log("line");
		}
	',
}

let publish_watch = (
	tg checkpoint watch process.control.response.publish --params '{"kind":"read"}'
	| from json
	| get watch
)
let published_watch = (
	tg checkpoint watch process.control.response.published --params '{"kind":"read"}'
	| from json
	| get watch
)
let timeout_watch = (
	tg checkpoint watch control.request.timeout
	| from json
	| get watch
)
let run = job spawn {
	let job_id = job id
	let output = tg run $path | complete
	$output | job send --tag $job_id 0
}

# Publish the read response while the requester is held after the timeout.
tg checkpoint wait control.request.timeout $timeout_watch 0 | ignore
tg checkpoint wait process.control.response.publish $publish_watch 0 | ignore
tg checkpoint continue process.control.response.publish $publish_watch 0
tg checkpoint wait process.control.response.published $published_watch 0 | ignore
tg checkpoint continue process.control.response.published $published_watch 0
tg checkpoint continue control.request.timeout $timeout_watch 0
tg checkpoint unwatch control.request.timeout $timeout_watch
tg checkpoint unwatch process.control.response.publish $publish_watch
tg checkpoint unwatch process.control.response.published $published_watch

let output = job recv --tag $run --timeout 10sec
success $output
assert ($output.stdout | str contains "line")
