use test.nu *

export def spawn_nats [--config: record, --listen-port: int] {
	if (which nats-server | is-empty) {
		skip_test 'this test requires nats-server'
	}
	let port = $listen_port | default (-1)
	let pid_path = mktemp
	let config_path = mktemp
	let log = mktemp
	($config | default {}) | to json | save --force $config_path
	let job = job spawn -d nats {
		^nats-server --addr 127.0.0.1 --port $port --http_port -1 --pid $pid_path --config $config_path out+err> $log
	}

	# Wait for NATS to log that it is ready, and read the ports that it chose from its log.
	wait_for_log { log: $log } 'Server is ready'
	let output = open --raw $log
	let monitor_port = $output | parse --regex 'Starting http monitor on 127\.0\.0\.1:(?<port>\d+)' | get 0.port
	let port = $output | parse --regex 'Listening for client connections on 127\.0\.0\.1:(?<port>\d+)' | get 0.port | into int
	let monitor_url = $'http://127.0.0.1:($monitor_port)'

	{ config_path: $config_path, job: $job, log: $log, pid_path: $pid_path, monitor_url: $monitor_url, port: $port, url: $'nats://127.0.0.1:($port)' }
}

# Stop NATS and wait for its process to exit.
export def stop_nats [nats: record] {
	let pid = open --raw $nats.pid_path | str trim | into int
	try { job kill $nats.job }
	wait_for_exit $pid 'NATS did not stop'
}

# Reload the NATS configuration and wait for NATS to log that it reloaded.
export def reload_nats [nats: record, config: record] {
	let pid = open --raw $nats.pid_path | str trim
	$config | to json | save --force $nats.config_path
	^kill -HUP ($pid | into string)
	wait_for_log { log: $nats.log } 'Reloaded server configuration'
}
