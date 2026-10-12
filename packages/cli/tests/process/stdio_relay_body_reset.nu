use ../lib/test.nu *

# A relay whose request body fails mid-stream is reopened, so every byte arrives exactly once.

const proxy_path = path self ../lib/stdio_reset_proxy.mjs

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}

# Route the runner's requests to the remote through a proxy that resets its first stdio write once.
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
let socket = $remote.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port write 1048576 $log $ready }
wait_until { $ready | path exists }
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $'http://127.0.0.1:($port)' } },
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
let line = "" | fill --character x --width 4194304
let output = $"($line)\n" | timeout 60s tg --url $local.url run --remote $path | complete
job kill $proxy
success $output
assert ((open --raw $log | lines | length) == 1) "the runner's stdio write must be reset"
let lines = $output.stdout | lines
assert equal ($lines | first) "4194304"
assert (($lines | get 1) == $line) "every byte must arrive exactly once"
