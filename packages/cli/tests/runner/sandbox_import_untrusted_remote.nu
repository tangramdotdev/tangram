use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sandbox on a runner with the VFS must see a file that its package imports when the runner does not trust its remote.
let remote = server spawn --name remote --config { roles: [api indexer scheduler] }
let created = tg --url $remote.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	vfs: true,
}
let path = artifact {
	tangram.ts: '
		import data from "./data.txt" with { type: "file" };
		export default () => tg.build({ args: ["-c", tg`test -f ${data}`], executable: "/bin/sh", host: tg.host.current }).sandbox(true);
	',
	data.txt: 'hello',
}
let output = tg --url $remote.url build $path | complete
success $output 'the sandbox should see the imported file'
