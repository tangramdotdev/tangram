use ../../util/test.nu *

# A read of a running process's piped stdout returns its output from the start, although the index does not have the process yet.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let connection = checkpoint watch runner.connection.accept --params { runner: $created.data.id } --token $root_token --url $remote.url
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			const child = await tg.spawn({
				args: ["-c", "echo hello; while :; do :; done"],
				executable: "sh",
				network: true,
			}).stdout("pipe").sandbox(false);
			await child.wait();
		};
	',
}

let id = checkin push $remote $path --token $root_token

# Let the scheduler finish recovery and accept the runner before holding the index writes that recovery awaits.
checkpoint pass $connection | ignore
checkpoint unwatch $connection

# Hold the index writes, so that the index does not have the child.
let write = checkpoint watch index.write --token $root_token --url $remote.url
let parent = tg --url $remote.url --token $root_token spawn --network --no-tokens $id | referent node
checkpoint wait $write | ignore

# Get the child from its parent's holder once the parent spawns it.
let children = ^timeout 30s tg --url $remote.url --token $root_token process children --no-timeout --length 1 $parent | from json
let child = $children | first | get process | split row '?' | first

# Read the child's stdout from the start.
let output = timeout 30s tg --url $remote.url --token $root_token process stdio read --stream stdout --length 6 --no-timeout $child | complete
success $output "the stdio read should succeed"
assert equal $output.stdout "hello\n" "the read should return the output from the start"

checkpoint unwatch $write
