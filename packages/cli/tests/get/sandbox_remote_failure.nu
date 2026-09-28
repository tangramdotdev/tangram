use ../lib/test.nu *

# Getting a sandbox succeeds when one remote has it, even if another remote fails.

let root_token = random chars
let remote_alpha = server spawn --cloud --name remote-alpha --preserve-keys --config {
	authentication: { root: { token: $root_token } },
}
let remote_zeta = server spawn --cloud --name remote-zeta
let created = tg --url $remote_alpha.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote_alpha.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: {
		alpha: { url: $remote_alpha.url }
		zeta: { url: $remote_zeta.url }
	}
}

let sandbox = tg --url $remote_alpha.url sandbox create --no-network | str trim

let pid = open ($remote_zeta.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the zeta remote should stop"

let output = tg --url $local.url get $sandbox | complete
success $output
assert equal ($output.stdout | from json | get data.id) $sandbox
