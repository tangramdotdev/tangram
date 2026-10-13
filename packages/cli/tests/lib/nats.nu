use test.nu *

export def spawn_nats [--config: record, --listen-port: int] {
	if (which nats-server | is-empty) {
		skip_test 'this test requires nats-server'
	}
	let port = if $listen_port == null { port } else { $listen_port }
	let monitor_port = port
	let pid_path = mktemp
	let config_path = mktemp
	($config | default {}) | to json | save --force $config_path
	let job = job spawn -d nats {
		bash -c $"
			PARENT_PID=\$PPID
			SELF_PID=\$\$
			\(
				while kill -0 \$PARENT_PID 2>/dev/null; do
					sleep 0.05
				done
				kill -TERM -\$SELF_PID 2>/dev/null || true
			\) &
			exec nats-server --addr 127.0.0.1 --port ($port) --http_port ($monitor_port) --pid \"($pid_path)\" --config \"($config_path)\"
		"
	}
	let monitor_url = $'http://127.0.0.1:($monitor_port)'
	wait_until { try { (http get $'($monitor_url)/healthz').status == 'ok' } catch { false } } 'NATS did not start'
	{ config_path: $config_path, job: $job, pid_path: $pid_path, monitor_url: $monitor_url, port: $port, url: $'nats://127.0.0.1:($port)' }
}

export def stop_nats [nats: record] {
	try { job kill $nats.job }
}

export def reload_nats [nats: record, config: record] {
	let pid = open --raw $nats.pid_path | str trim
	$config | to json | save --force $nats.config_path
	^kill -HUP ($pid | into string)
}
