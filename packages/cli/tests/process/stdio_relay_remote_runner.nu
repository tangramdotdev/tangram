use ../lib/test.nu *

# Piped stdin and stdout cross servers over the relay when the process runs on a remote runner.

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

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`read line; printf "%s\\n%s\\n" "\${#line}" "\$line"`.sandbox().stdio("inherit");
		}
	',
}

let line = "" | fill --character x --width 4194304
let output = $"($line)\n" | timeout 60s tg --url $local.url run --remote $path | complete
success $output
let lines = $output.stdout | lines
assert equal ($lines | first) "4194304"
assert (($lines | get 1) == $line) "the process must echo the line"
