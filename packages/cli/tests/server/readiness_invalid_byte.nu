use ../lib/test.nu *

# A server that signals an invalid readiness byte is rejected and its readiness FIFO is removed.

let path = mktemp --directory
let bin_path = $path | path join 'bin'
mkdir $bin_path
let tangram_path = $bin_path | path join 'tangram'
'#!/bin/sh
printf "\001" >&3
' | save $tangram_path
chmod +x $tangram_path

let config_path = $path | path join 'config.json'
'{}' | save $config_path
let log_path = $path | path join 'log'
touch $log_path
let server = {
	config: {},
	config_path: $config_path,
	directory: ($path | path join 'server'),
	job: null,
	log: $log_path,
	name: 'server',
	url: 'http://127.0.0.1:1',
}

let error = with-env { PATH: ($env.PATH | prepend $bin_path) } {
	try {
		server start $server | ignore
		null
	} catch { |error| $error }
}

assert ($error != null) 'expected the invalid readiness byte to fail'
snapshot $error.msg 'the server signaled an invalid readiness byte: 1'
assert not (($path | path join 'ready') | path exists) 'expected the readiness FIFO to be removed'
