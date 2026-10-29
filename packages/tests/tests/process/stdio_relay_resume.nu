use ../../util/test.nu *

# A relay that fails mid-stream is replaced by one that resumes from the delivered position, so every byte arrives exactly once.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`read line; printf "%s\\n%s\\n" "\${#line}" "\$line"`.sandbox().stdio("inherit");
		}
	',
}

# Drop the runner's stdout relay write and then its stdin relay read, each once.
for case in [
	{ checkpoint: process.stdio.write.relay.data, reopen: process.stdio.read.reopen }
	{ checkpoint: process.stdio.read.relay.data, reopen: process.stdio.write.reopen }
] {
	let checkpoint = $case.checkpoint
	let watch = tg --url $remote.url --token $root_token checkpoint watch $checkpoint | from json | get watch
	let reopen_watch = tg --url $remote.url --token $root_token checkpoint watch $case.reopen | from json | get watch
	let line = "" | fill --character x --width 4194304
	let run = job spawn {
		let job_id = job id
		let output = $"($line)\n" | timeout 60s tg --url $local.url run --remote $path | complete
		$output | job send --tag $job_id 0
	}
	# Let several items through before failing the relay, so the new relay must skip what was already delivered.
	for index in 0..4 {
		tg --url $remote.url --token $root_token checkpoint wait --timeout 30s $checkpoint $watch $index | ignore
		tg --url $remote.url --token $root_token checkpoint continue $checkpoint $watch $index
	}
	let hit = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s $checkpoint $watch 5 | from json
	tg --url $remote.url --token $root_token checkpoint panic $checkpoint --params ({ open: $hit.params.open } | to json --raw)
	tg --url $remote.url --token $root_token checkpoint unwatch $checkpoint $watch

	# The failed relay must be reopened.
	success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s $case.reopen $reopen_watch 0 | complete) "the relay must be reopened"
	tg --url $remote.url --token $root_token checkpoint unwatch $case.reopen $reopen_watch
	let output = job recv --tag $run --timeout 60sec
	success $output
	let lines = $output.stdout | lines
	assert equal ($lines | first) "4194304"
	assert (($lines | get 1) == $line) "every byte must arrive exactly once"
}
