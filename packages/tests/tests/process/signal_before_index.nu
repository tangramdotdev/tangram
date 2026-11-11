use ../../util/test.nu *

# A signal sent to a process that the index does not have yet reaches the process once, although the signal is sent to the process's holder both before and after the index has the process.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let registration = checkpoint watch runner.registration.accept --params { runner: $created.data.id } --token $root_token --url $remote.url
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			const child = await tg.spawn({
				args: ["-c", "n=0; trap \"n=\\$((n + 1))\" USR1; trap \"exit \\$n\" TERM; while :; do :; done"],
				executable: "sh",
				network: true,
			}).sandbox(false);
			console.log(child.id);
			await child.wait();
		};
	',
}

let id = checkin push $remote $path --token $root_token

# Let the scheduler finish recovery and accept the runner before holding the index writes that recovery awaits.
checkpoint pass $registration | ignore
checkpoint unwatch $registration

# Hold the index writes of the child, so that the index does not have it.
let write = checkpoint watch index.write --token $root_token --url $remote.url
let parent = tg --url $remote.url --token $root_token spawn --network --no-tokens $id | referent node
checkpoint wait $write | ignore
mut children = []
while ($children | is-empty) {
	$children = (^timeout 10s tg --url $remote.url --token $root_token process children $parent | from json)
	sleep 200ms
}
let child = $children | first | get process | split row '?' | first
sleep 2sec

# Signal the child, and hold the holder's reply, so that the signal's read of the index finds no child, after which the signal is sent again once the index has the child.
let response = checkpoint watch process.control.response.publish --params { kind: signal, process: $child } --token $root_token --url $remote.url
let index = checkpoint watch process.signal.index --params { process: $child } --token $root_token --url $remote.url
let url = $remote.url
let signal = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token signal --signal USR1 $child | complete | job send --tag $id 0
}
checkpoint wait $response | ignore
checkpoint pass $index | ignore

# Let the index have the child, and then let the holder reply.
checkpoint unwatch $write
checkpoint pass $index 1 | ignore
checkpoint unwatch $index
sleep 1sec
checkpoint unwatch $response
success (job recv --tag $signal --timeout 60sec) "the signal should succeed"

# Stop the child, which exits with the number of signals that it received.
sleep 1sec
tg --url $remote.url --token $root_token signal --signal TERM $child
let outcome = timeout 30s tg --url $remote.url --token $root_token wait $child | from json
assert equal $outcome.exit 1 "the child should receive the signal once"
