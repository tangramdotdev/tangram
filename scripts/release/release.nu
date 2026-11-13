#!/usr/bin/env nu

use ./common.nu *

# Build the release archives for every target that can be built from this machine, check them out to release/, and verify them.
def main [
	--plan # Print which targets would be built and why the others would be skipped, then exit.
	...target: string # The targets to build, which skips the check of what this machine can build. Defaults to every buildable target.
] {
	# Plan the build.
	let machine = { arch: (host_arch), os: (host_os) }
	let routes = $targets | items { |triple, entry| { target: $triple } | merge (route $machine $entry) }
	let selected = if ($target | is-empty) {
		$routes | where buildable | get target
	} else {
		let unknown = $target | where { |triple| $triple not-in ($targets | columns) }
		if ($unknown | is-not-empty) {
			error make { msg: $'unsupported target: ($unknown | str join ", ")', help: $'The targets are ($targets | columns | str join ", ").' }
		}
		$target
	}
	print -e $'machine ($machine.arch)-($machine.os)'
	for row in $routes {
		let status = if $row.target in $selected { 'build' } else { 'skip ' }
		print -e $'($status)  ($row.target)  [($row.reason)]'
	}
	if $plan {
		return
	}
	if ($selected | is-empty) {
		error make { msg: 'this machine cannot build any release target' }
	}

	# Build the archives and the checksums, and check them out.
	let root = repository
	cd $root
	let commit = commit
	let release_path = $root | path join release
	step $'Building tangram (version)+($commit) for ($selected | str join ", ")'
	let arg = { commit: $commit, targets: $selected } | to json --raw
	^tg build ./build.tg.ts#release --arg-value $arg $'--checkout=($release_path)' --checkout-force

	# Verify the checksums and each archive.
	step 'Verifying the checksums'
	do { cd $release_path; ^sha256sum --check SHASUMS256.txt }
	mut failures = []
	for triple in $selected {
		let entry = $targets | get $triple
		let archive_path = $release_path | path join $'tangram_($entry.host).tar.gz'
		step $'Verifying ($archive_path | path basename)'
		let args = if $entry.os == 'darwin' { ['--skip-signing'] } else { [] }
		let result = ^nu ($root | path join scripts/release/verify.nu) ...$args $archive_path | complete
		print $result.stdout
		if $result.exit_code != 0 {
			$failures ++= [$triple]
		}
	}

	# Report the result.
	let skipped = $routes | where { |row| $row.target not-in $selected }
	print ''
	print $'built     ($selected | str join ", ")'
	for row in $skipped {
		print $'skipped   ($row.target): ($row.reason)'
	}
	if ($failures | is-not-empty) {
		print $'FAILED    ($failures | str join ", ")'
		exit 1
	}
	print $'wrote ($release_path)'
}

# Determine whether this machine can build a target, mirroring how the release recipe in build.tg.ts chooses each target's build machine.
def route [machine: record, entry: record]: nothing -> record {
	# Build a target of this machine's platform natively.
	if $entry.os == $machine.os and $entry.arch == $machine.arch {
		return { buildable: true, reason: 'native' }
	}

	# Cross-compile another Linux target on a Linux machine.
	if $entry.os == 'linux' and $machine.os == 'linux' {
		return { buildable: false, reason: 'cross-compiled on this machine, but std.sdk.standalone does not support cross-compilation yet' }
	}

	# Build any other target on a remote runner of its platform.
	{ buildable: false, reason: $'built on a remote ($entry.arch)-($entry.os) runner, which must be registered before this target can be built' }
}
