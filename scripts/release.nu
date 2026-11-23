#!/usr/bin/env nu

const stages = [compile package publish]
const stable_version = '^(0|[1-9][0-9]*)[.](0|[1-9][0-9]*)[.](0|[1-9][0-9]*)$'
const targets = [
	aarch64-darwin
	aarch64-linux-gnu
	aarch64-linux-musl
	aarch64-linux-musl-static
	x86_64-darwin
	x86_64-linux-gnu
	x86_64-linux-musl
	x86_64-linux-musl-static
]

# Build and package Tangram with --cargo, or package staged binaries by default.
# Repeat --stage <stage> and --target <target> to select stages and targets.
def --wrapped main [...args: string] {
	mut cargo = false
	mut sign = true
	mut selected_stages = []
	mut selected_targets = []
	mut version = ''
	mut remaining = $args
	while (not ($remaining | is-empty)) {
		let flag = $remaining | first
		$remaining = $remaining | skip 1
		match $flag {
			'-h' | '--help' => {
				print 'Usage: tg run .#release -- [--cargo] [--no-sign] [--stage <compile|package|publish>] [--target <target>] [--version <version>]

Repeat --stage or --target to select more than one.
The default stage is package, or compile followed by package with --cargo.
The package stage signs Linux repositories using the authenticated cf CLI.
Packaging a macOS target requires a Mac and includes Tangram.dmg.
Downloaded keys are stored temporarily under keys/. Use --no-sign for local unsigned packages.
Publishing requires --version, a matching GitHub tag, and signed packaging for the selected targets.
Existing assets can be replaced, including for version 0.0.0.'
				return
			}
			'--cargo' => { $cargo = true }
			'--no-sign' => { $sign = false }
			'--stage' => {
				let stage = $remaining | get -o 0
				if $stage not-in $stages {
					error make {msg: $'expected --stage followed by one of ($stages | str join ", ")'}
				}
				$selected_stages = $selected_stages | append $stage
				$remaining = $remaining | skip 1
			}
			'--target' => {
				let target = $remaining | get -o 0
				if $target not-in $targets {
					error make {msg: $'expected --target followed by a release target ($targets | str join ", ")'}
				}
				$selected_targets = $selected_targets | append $target
				$remaining = $remaining | skip 1
			}
			'--version' => {
				let value = $remaining | get -o 0 | default ''
				if $value !~ '^[0-9][0-9A-Za-z.+~-]*$' {
					error make {msg: 'expected --version followed by a package version'}
				}
				$version = $value
				$remaining = $remaining | skip 1
			}
			_ => { error make {msg: $'unknown release argument ($flag)'} }
		}
	}
	let selected_stages = if ($selected_stages | is-empty) {
		if $cargo { [compile package] } else { [package] }
	} else {
		$selected_stages
	}
	let selected_targets = $selected_targets | uniq
	if 'publish' in $selected_stages {
		if $version !~ $stable_version {
			error make {msg: 'publishing requires --version with a numeric release version'}
		}
		if not $sign {
			error make {msg: 'publishing requires the signed package stage'}
		}
	}
	for stage in $stages {
		if $stage in $selected_stages {
			match $stage {
				compile => { compile $cargo $selected_targets }
				package => { package $selected_targets $version $sign }
				publish => { ^python3 scripts/release/publish.py publish $version ...$selected_targets }
			}
		}
	}
}

def compile [cargo: bool, selected_targets: list<string>] {
	if not $cargo {
		error make {msg: 'the compile stage requires --cargo'}
	}
	let host = ^rustc -vV | lines | where {|line| $line starts-with 'host: '} | first | str replace 'host: ' ''
	let target = $host | str replace '-apple-' '-' | str replace '-unknown-' '-'
	if $target not-in $targets {
		error make {msg: $'unsupported Cargo host ($host)'}
	}
	let possible_targets = if ($target ends-with '-musl') { [$target $'($target)-static'] } else { [$target] }
	if (not ($selected_targets | is-empty)) and not ($possible_targets | any {|target| $target in $selected_targets }) {
		error make {msg: $'--cargo builds ($target), which was not selected with --target'}
	}

	^cargo build --release -p tangram_cli --bin tangram
	let directory = ^cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let binary = $directory | path join release tangram
	let target = if ($target ends-with '-musl') {
		let headers = ^readelf -l $binary
		if ($headers | str contains 'INTERP') { $target } else { $'($target)-static' }
	} else {
		$target
	}
	if (not ($selected_targets | is-empty)) and $target not-in $selected_targets {
		error make {msg: $'--cargo builds ($target), which was not selected with --target'}
	}

	mkdir release
	let destination = staged-binary $target
	let temporary = ^mktemp -d release/.compile.XXXXXXXX | str trim
	try {
		let staged = $temporary | path join tangram
		^install -m 755 $binary $staged
		place-file $staged $destination
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
	print $'staged ($destination)'
}

def package [selected_targets: list<string>, version: string, sign: bool] {
	let selected_targets = if ($selected_targets | is-empty) {
		$targets | where {|target| (staged-binary $target | path type) == file }
	} else {
		$selected_targets
	}
	if ($selected_targets | is-empty) {
		error make {msg: 'no Tangram binaries are staged in release/'}
	}
	let version = if ($version | is-empty) { open Cargo.toml | get workspace.package.version } else { $version }
	let macos_app = ($selected_targets | any {|target| $target ends-with '-darwin'})
	if $macos_app and $nu.os-info.name != 'macos' {
		error make {msg: 'packaging the macOS app requires macOS'}
	}
	if $macos_app and $version !~ $stable_version {
		error make {msg: 'packaging the macOS app requires a numeric release version'}
	}
	for target in $selected_targets {
		let binary = staged-binary $target
		if ($binary | path type) != file or (^test -x $binary | complete).exit_code != 0 {
			error make {msg: $'expected a staged executable at ($binary)'}
		}
	}
	if $macos_app {
		let sign_flags = if $sign { [] } else { ['--no-sign'] }
		^nu scripts/macos/release.nu --version $version ...$sign_flags
	}
	let manifest = ('release/manifest.json' | path expand)
	if ($manifest | path type) == file {
		rm $manifest
	}
	if $macos_app {
		let staged = ^mktemp release/.dmg.XXXXXXXX | str trim
		try {
			^install -m 644 packages/macos/build/Distribution/Tangram.dmg $staged
			place-file $staged release/Tangram.dmg
		} catch {|error|
			rm -f $staged
			error make $error.raw
		}
	}
	for target in $selected_targets {
		let binary = staged-binary $target
		let archive = $'($binary).tar.gz'
		let temporary = ^mktemp -d release/.package.XXXXXXXX | str trim
		try {
			let output = $temporary | path join archive.tar.gz
			let wrapper = $temporary | path join tgx
			^ln $binary ($temporary | path join tangram)
			^ln -s tangram ($temporary | path join tg)
			'#!/bin/sh
exec tg run -b "$@"
' | save $wrapper
			^chmod 755 $wrapper
			with-env { COPYFILE_DISABLE: '1' } {
				^tar --no-xattrs -czf $output -C $temporary tangram tg tgx
			}
			place-file $output $archive
		} catch {|error|
			rm -r $temporary
			error make $error.raw
		}
		rm -r $temporary
		print $'packaged ($archive)'
		match $target {
			aarch64-linux-gnu | x86_64-linux-gnu => {
				let architectures = if $target == aarch64-linux-gnu {
					{deb: 'arm64', rpm: 'aarch64'}
				} else {
					{deb: 'amd64', rpm: 'x86_64'}
				}
				^nu packages/linux/deb/package.nu $archive $architectures.deb $version
				^nu packages/linux/rpm/package.nu $archive $architectures.rpm $version
			}
			aarch64-linux-musl | x86_64-linux-musl => {
				let architecture = $target | str replace '-linux-musl' ''
				^nu packages/linux/apk/package.nu $archive $architecture $version
			}
			_ => {}
		}
	}
	let gnu_targets = $selected_targets | where {|target| $target ends-with '-linux-gnu' }
	if not ($gnu_targets | is-empty) {
		^python3 scripts/release/arch_package.py $version ...$gnu_targets
	}
	if $sign {
		^python3 scripts/release/sign.py $version ...$selected_targets
		if $version =~ $stable_version {
			^python3 scripts/release/publish.py manifest $version ...$selected_targets
		}
	}
}

def staged-binary [target: string] {
	$'release/tangram_($target)'
}

def place-file [source: path, destination: path] {
	if ($destination | path type) == dir {
		error make {msg: $'the release destination is a directory ($destination)'}
	}
	mv -f $source $destination
}
