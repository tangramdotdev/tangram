use ../../test.nu *

export def skip_unless_supported [] {
	if $nu.os-info.name == 'macos' and (($env.TANGRAM_TEST_FSKIT? | default '') | str length) == 0 {
		skip_test 'the macos vfs tests require --fskit'
	}
}

export def artifacts_path [server_path: string] {
	$server_path | path join 'artifacts' | path expand
}

export def assert_mounted [server_path: string] {
	let path = artifacts_path $server_path
	let mounted = match $nu.os-info.name {
		'linux' => ((do --ignore-errors { ^mountpoint -q $path; $env.LAST_EXIT_CODE }) == 0),
		'macos' => (^mount | lines | any { |line|
			let matches = ($line | parse --regex '^.+ on (?<target>.+) \(')
			(not ($matches | is-empty)) and (($matches | first | get target) == $path)
		}),
		_ => false,
	}
	assert $mounted $'expected ($path) to be mounted as a vfs'
}

export def root [server_path: string, id: string] {
	artifacts_path $server_path | path join $id
}

export def assert_read_only [output: record, operation: string] {
	assert ($output.exit_code != 0) $'expected ($operation) to fail'
	let messages = ['Permission denied', 'Read-only file system']
	let read_only = $messages | any { |message| $output.stderr | str contains $message }
	assert $read_only $'expected ($operation) to fail because the VFS is read-only, got: ($output.stderr | str trim)'
}
