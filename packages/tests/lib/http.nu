use test.nu *

const http_path = path self http.ts

# Serve fixed responses keyed by URL path. Each response accepts body, file, headers, and status fields.
export def --env spawn_http_server [routes: record] {
	let routes_path = mktemp
	$routes | to json | save --force $routes_path
	let port_path = mktemp

	# Supervise the HTTP process so that cleanup stops it, and wait for it to signal readiness after writing its port.
	let supervisor = mktemp -d
	track_supervisor $supervisor
	try {
		^tangram_tests_supervisor start --directory $supervisor --name http --ready-fd 3 -- bun run $http_path $routes_path $port_path o> /dev/null
	} catch {
		error make { msg: 'the local HTTP server must start' }
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
		supervisor: $supervisor,
		url: $'http://($hostname):($port)',
	}
}
