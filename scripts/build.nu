#!/usr/bin/env nu

const repository_path = path self '../'

# Build the workspace and optionally grant the CLI the Linux mount capability.
def --wrapped main [
	--cap-sys-admin # Apply cap_sys_admin=ep to the built CLI using sudo.
	...args: string # Arguments to pass to cargo build.
] {
	cd $repository_path
	if not $cap_sys_admin {
		exec cargo build --all-features ...$args
	}
	if $nu.os-info.name != 'linux' {
		error make { msg: '--cap-sys-admin is only supported on Linux' }
	}

	# Find the CLI artifacts using Cargo's selected profile and target directories.
	let binaries = (
		do --capture-errors {
			cargo build --all-features ...$args --message-format=json-render-diagnostics
		}
		| lines
		| each { from json }
		| where reason == 'compiler-artifact'
		| where profile.test == false
		| where { |message|
			$message.target.name == 'tangram' and 'bin' in $message.target.kind and $message.executable != null
		}
		| get executable
		| uniq
	)
	if ($binaries | is-empty) {
		error make { msg: 'the build did not produce a tangram CLI executable' }
	}

	# Apply and display the capability after the build succeeds.
	for binary in $binaries {
		sudo setcap cap_sys_admin=ep $binary
		getcap $binary
	}
}
