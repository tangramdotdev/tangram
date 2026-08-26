use ../../test.nu *

# A build succeeds when a configured remote is unreachable, because consulting a remote for a cached process is an optimization and not a prerequisite.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact {
	tangram.ts: '
		export default function () { return tg.build(child); }
		export function child() { return "hello"; }
	'
}

# Kill the remote server.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } "the remote should stop"

# Require a cached process while the remote is unreachable.
let cached_output = tg build --cached $path | complete
failure $cached_output "a cache-only build should fail while the remote is unreachable"
assert ($cached_output.stderr | str contains "failed to get a cached process from a remote") "the cache-only build should surface the remote error"

# Build while the remote is unreachable.
let output = tg build $path | complete
success $output "the build should succeed while the remote is unreachable"
snapshot ($output.stdout | str trim) '"hello"'
