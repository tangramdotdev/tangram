use ../../lib/test.nu *

# A runner and a local server whose clients spread requests over several connections to an instance with several servers send each process's correlated requests over one connection, so that every server answers them over its local channel instead of the messenger.

const proxy_path = path self ../../lib/round_robin_proxy.mjs

# Spawn two servers in one instance, and route connections to them in turn, like a layer 4 load balancer.
let root_token = random chars
let instance = instance --cloud
let config = {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let first = server spawn --preserve-keys --instance $instance --name first --config $config
let second = server spawn --preserve-keys --instance $instance --name second --config $config
let port = port
let ready = mktemp -d | path join ready
^mkfifo $ready
let sockets = [$first $second] | each { |server| $server.directory | path join socket }
let proxy = job spawn { node $proxy_path $port $ready ...$sockets }
success (timeout 30s cat $ready | complete) "the proxy should listen"
let url = $'http://127.0.0.1:($port)'

# Hold every correlated request that either server answers over the messenger.
let checkpoints = [process.attach.service.messenger process.control.service.messenger sandbox.control.service.messenger]
let watches = [$first $second] | each { |server|
	$checkpoints | each { |name| checkpoint watch $name --url $server.url --token $root_token }
} | flatten

# Spawn a runner and a local server whose clients open a connection for every concurrent request.
let client = { pool: { max: 8, shared: 1 } }
let created = tg --url $first.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remote_client: $client,
	remotes: { default: { token: $created.token.token, url: $url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $first.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remote_client: $client,
	remotes: { default: { token: $alice.token, url: $url } },
}

# Run sandboxed processes through the local server's attach, so that each one has attach, control, and sandbox control requests to correlate.
let path = artifact { tangram.ts: 'export default () => tg.file("hello")' }
for _ in 1..4 {
	let output = timeout 60s tg --url $local.url build --remote $path | complete
	success $output "the build should complete without a correlated request reaching the messenger"
	let read = tg --url $local.url read ($output.stdout | str trim) | complete
	success $read
	assert equal $read.stdout hello
}

$watches | each { |watch| checkpoint unwatch $watch } | ignore
job kill $proxy
