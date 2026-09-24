use ../lib/test.nu *

const helper = path self sync_control.py

export def test [case: string] {
	if (which nats-server | is-empty) {
		skip_test 'this test requires nats-server'
	}
	let port = port
	let log = mktemp
	let messenger = job spawn {
		^nats-server --addr 127.0.0.1 --port $port out+err> $log
	}
	wait_until { open --raw $log | str contains 'Server is ready' } 'NATS must start'
	let store = { object_concurrency: 8, object_max_batch: 1 }
	let server = server spawn --config {
		advanced: { checkpoints: true },
		messenger: { kind: 'nats', url: $'nats://127.0.0.1:($port)' },
		sync: {
			control: {
				heartbeat_interval: 0.1,
				lease_ttl: 2,
				recovery_timeout: 0.5,
				request_timeout: 5,
				retry_interval: (if $case == 'notification' { 2 } else { 0.05 }),
			},
			get: {
				checkout_pointers: false,
				store: { lmdb: $store, memory: $store, scylla: $store },
			},
			put: { store: { object_batch_size: 1024 } },
		},
	}
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let source = server spawn --name source
	let output = python3 $helper $case $socket (which tg | first | get path) $server.url $port $source.url $server.directory | complete
	server stop $source
	server stop $server
	job kill $messenger
	success $output $'the sync control ($case) case should pass'
}
