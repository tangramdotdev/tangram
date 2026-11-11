use ../../util/test.nu *

# A read of a process's children reaches the process's holder when the holder starts the process during the read, although the index does not have the process.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	control: { read_timeout: 30 },
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
			while (true) {
				await tg.sleep(1);
			}
		};
	',
}

let id = checkin push $remote $path --token $root_token

# Let the scheduler finish recovery and accept the runner before holding the index writes that recovery awaits.
checkpoint pass $registration | ignore
checkpoint unwatch $registration

# Hold the index writes, so that the index does not have the process, and hold the start of the process at its holder.
let write = checkpoint watch index.write --token $root_token --url $remote.url
let start = checkpoint watch process.control.index.started --token $root_token --url $remote.url
let parent = tg --url $remote.url --token $root_token spawn --no-tokens $id | referent node
checkpoint wait $start | ignore

# Read the children, and let the holder start the process once the read has sent its request.
let url = $remote.url
let read = job spawn {
	let id = job id
	timeout 20s tg --url $url --token $root_token process children $parent | complete | job send --tag $id 0
}
sleep 1sec
checkpoint unwatch $start

# The holder answers the read.
let output = job recv --tag $read --timeout 30sec
success $output "the read should get the children from the holder"
assert equal ($output.stdout | from json) [] "the process should have no children"
checkpoint unwatch $write
