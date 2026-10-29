use ../../util/test.nu *

# A client whose stdin write or stdout read stream is reset mid-stream reconnects and resumes, so every byte arrives exactly once.

const proxy_path = path self ../../util/stdio_reset_proxy.mjs

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
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
let solo = server spawn --name solo

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`read line; printf "%s\\n%s\\n" "\${#line}" "\$line"`.sandbox().stdio("inherit");
		}
	',
}

# Stream the CLI's stdio through a proxy that resets the stream once, for a root process on the same server and for a remote root process.
for case in [[args kind server]; [[] write $solo] [[] read $solo] [[--remote] write $local] [[--remote] read $local]] {
	let port = port
	let log = mktemp
	let ready = mktemp -d | path join ready
	^mkfifo $ready
	let socket = $case.server.directory | path join socket
	let proxy = job spawn { node $proxy_path $socket $port $case.kind 1048576 $log $ready }
	success (timeout 30s cat $ready | complete) "the proxy should listen"
	let line = "" | fill --character x --width 4194304
	let output = $"($line)\n" | timeout 60s tg --url $'http://127.0.0.1:($port)' run ...$case.args $path | complete
	job kill $proxy
	success $output
	assert ((open --raw $log | lines | length) == 1) $"the stdio ($case.kind) must be reset"
	let lines = $output.stdout | lines
	assert equal ($lines | first) "4194304"
	assert (($lines | get 1) == $line) "every byte must arrive exactly once"
}
