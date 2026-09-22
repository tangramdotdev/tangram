use ../lib/test.nu *

# Server starts record cleanup intent for explicit, instance, and restart VFS configurations, while non-VFS starts do not.

let path = mktemp --directory
let bin_path = $path | path join 'bin'
mkdir $bin_path
let tangram_path = $bin_path | path join 'tangram'
'#!/bin/sh
printf "\000" >&3
' | save $tangram_path
chmod +x $tangram_path
let marker_path = $env.TMPDIR | path join '.tangram_test_vfs_cleanup'

def server_record [path: path, config: record] {
	let config_path = $path | path join 'config.json'
	$config | to json | save --force $config_path
	let log_path = $path | path join 'log'
	touch $log_path

	{
		config: $config,
		config_path: $config_path,
		directory: ($path | path join 'server'),
		job: null,
		log: $log_path,
		name: 'server',
		url: 'http://127.0.0.1:1',
	}
}

with-env { PATH: ($env.PATH | prepend $bin_path) } {
	let plain_path = $path | path join 'plain'
	mkdir $plain_path
	server start (server_record $plain_path {}) | ignore
	assert not ($marker_path | path exists) 'expected a non-VFS server not to mark cleanup intent'

	let explicit_path = $path | path join 'explicit'
	mkdir $explicit_path
	server start (server_record $explicit_path { vfs: true }) | ignore
	assert ($marker_path | path exists) 'expected an explicit VFS config to mark cleanup intent'
	rm $marker_path

	let configured_path = $path | path join 'configured'
	mkdir $configured_path
	let server = server_record $configured_path { vfs: false }
	{ vfs: { kind: 'fuse' } } | to json | save --force $server.config_path
	server restart $server | ignore
	assert ($marker_path | path exists) 'expected a restart config file to mark cleanup intent'
	rm $marker_path

	let instance = instance --config { vfs: true }
	server spawn --instance $instance | ignore
	assert ($marker_path | path exists) 'expected an instance VFS config to mark cleanup intent'
}
