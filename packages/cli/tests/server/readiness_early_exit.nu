use ../../test.nu *

# A server that exits before signaling readiness reports its output immediately instead of waiting for the readiness timeout.

let path = mktemp --directory
let bin_path = $path | path join 'bin'
mkdir $bin_path
let tangram_path = $bin_path | path join 'tangram'
'#!/bin/sh
echo "intentional startup failure" >&2
exit 42
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

let start = date now
let error = with-env { PATH: ($env.PATH | prepend $bin_path) } {
	try {
		server start $server | ignore
		null
	} catch { |error| $error }
}
let elapsed = (date now) - $start

assert ($error != null) 'expected the server start to fail'
snapshot $error.msg 'the server exited before signaling readiness'
snapshot (open --raw $log_path | str trim) 'intentional startup failure'
assert ($elapsed < 5sec) 'expected the server exit to interrupt the readiness wait'
assert not (($path | path join 'ready') | path exists) 'expected the readiness FIFO to be removed'
