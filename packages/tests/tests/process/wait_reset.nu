use ../../util/test.nu *

# A wait on a remote survives a reset of its stream after the response started.

const proxy_path = path self ../../util/process_attach_proxy.js
let remote = server spawn --name remote
let port = port
let log = mktemp
let ready = mktemp -d | path join ready
^mkfifo $ready
let socket = $remote.directory | path join socket
let proxy = job spawn { node $proxy_path $socket $port $log $ready reset-body-first:wait }
success (timeout 30s cat $ready | complete) "the proxy should listen"
let local = server spawn --name local --config { remotes: { default: { url: $'http://127.0.0.1:($port)' } } }

# Build a process on the remote.
let path = artifact {
	tangram.ts: 'export default () => "output";'
}
let process = tg --url $remote.url build --detach $path | referent node
success (tg --url $remote.url wait $process | complete) "the build should finish"

# Wait through the proxy, which resets the first wait's stream after its first byte.
let output = tg --url $local.url wait $process | complete
success $output "the wait should survive the reset"
assert equal ($output.stdout | from json | get output) output
let resets = open $log | lines | where { str starts-with 'reset ' }
assert equal $resets ['reset body wait'] "the proxy should reset the wait's stream once"
job kill $proxy
