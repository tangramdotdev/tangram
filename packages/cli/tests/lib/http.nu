use test.nu *

const http_path = path self http.ts

# Serve fixed responses keyed by URL path. Each response accepts body, file, headers, and status fields.
export def --env spawn_http_server [routes: record] {
	let routes_path = mktemp
	$routes | to json | save --force $routes_path
	let port_path = mktemp
	let status_path = mktemp
	let log_path = mktemp
	# Use the server lifecycle so cleanup waits for the supervisor to reap the HTTP process.
	let exits = $env.TMPDIR? | default ($nu.temp-dir? | default $nu.temp-path?) | path join 'server_jobs'
	mkdir $exits
	let job = job spawn --description http {
		let job_id = job id
		let exit_path = $exits | path join $'($job_id).exit'
		'' | save --force $exit_path
		'' | save --force $status_path
		do -i {
			bash -c (process_supervisor) _ $nu.pid $status_path bun run $http_path $routes_path $port_path o+e> $log_path
		}
		let status = open --raw $status_path | str trim
		$"($status)\n" | save --force $exit_path
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
	let endpoint = open --raw $port_path | from json
	let port = $endpoint.port
	let hostname = if $nu.os-info.name == 'linux' and (id -u | str trim) != '0' {
		# Map a test-only address to host loopback for container and VM downloads.
		let bin = mktemp -d
		for executable in [pasta passt] {
			let path = which $executable | get --optional 0.path
			if $path == null {
				continue
			}
			let wrapper = $bin | path join $executable
			$'#!/bin/sh
exec "($path)" --map-host-loopback 169.254.254.254 "$@"
' | save --force $wrapper
			chmod +x $wrapper
		}
		$env.PATH = $env.PATH | prepend $bin
		'169.254.254.254'
	} else if $nu.os-info.name == 'linux' {
		$endpoint.hostname
	} else {
		'127.0.0.1'
	}
	{
		host_url: $'http://127.0.0.1:($port)',
		job: $job,
		url: $'http://($hostname):($port)',
	}
}
