use ../../util/test.nu *

# A status read and a children read on a remote survive a reset of their streams.

const proxy_path = path self ../../util/process_attach_proxy.js
let remote = server spawn --name remote
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
^mkfifo $ready
let socket = $remote.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port $log $ready reset-first:status,children }
success (timeout 30s cat $ready | complete) "the proxy should listen"
let local = server spawn --name local --config { remotes: { default: { url: $'http://127.0.0.1:($port)' } } }

# Build a process with a child on the remote.
let path = artifact {
	tangram.ts: '
		export default async () => await tg.build(child);
		export const child = () => "child";
	'
}
let process = tg --url $remote.url build --detach $path | referent node
success (tg --url $remote.url wait $process | complete) "the build should finish"

# Read the status and the children through the proxy, which resets the first request of each.
let output = tg --url $local.url process status --timeout 0 $process | complete
success $output "the status read should survive the reset"
assert equal ($output.stdout | from json | last) finished
let output = tg --url $local.url process children $process | complete
success $output "the children read should survive the reset"
assert equal ($output.stdout | from json | length) 1
let resets = open $log | lines | where { str starts-with 'reset ' }
assert equal ($resets | sort) ['reset children' 'reset status'] "the proxy should reset each read once"
job kill $proxy
