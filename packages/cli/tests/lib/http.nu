use test.nu *

const http_path = path self http.ts

# Serve fixed responses keyed by URL path. Each response accepts body, file, headers, and status fields.
export def spawn_http_server [routes: record] {
	let routes_path = mktemp
	$routes | to json | save --force $routes_path
	let port_path = mktemp
	let status_path = mktemp
	let log_path = mktemp
	let job = job spawn --description http {
		bash -c (process_supervisor) _ $nu.pid $status_path bun run $http_path $routes_path $port_path o+e> $log_path
	}
	try {
		wait_until {
			let status = open --raw $status_path | str trim
			if ($status | is-not-empty) {
				error make { msg: $'the local HTTP server exited during startup with status ($status)' }
			}
			open --raw $port_path | str trim | is-not-empty
		} 'the local HTTP server must start'
	} catch { |error|
		error make { msg: $error.msg, help: (open --raw $log_path) }
	}
	let port = open --raw $port_path | str trim
	{
		job: $job,
		url: $'http://127.0.0.1:($port)',
	}
}
