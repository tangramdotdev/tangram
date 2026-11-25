#!/usr/bin/env nu

use ./release/stage.nu

const stages = [compile push pull package publish]
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
const tangram_targets = [aarch64-darwin aarch64-linux-gnu x86_64-darwin x86_64-linux-gnu]
const tag_prefix = 'tangram/release'

# Build and package Tangram with --cargo, build it with Tangram with --stage compile, move staged binaries between machines with --stage push and --stage pull, or package staged binaries by default.
# Repeat --stage <stage> and --target <target> to select stages and targets.
def --wrapped main [...args: string] {
	mut cargo = false
	mut force = false
	mut retry = false
	mut sign = true
	mut selected_stages = []
	mut selected_targets = []
	mut version = ''
	mut build_number = ''
	mut remaining = $args
	while (not ($remaining | is-empty)) {
		let flag = $remaining | first
		$remaining = $remaining | skip 1
		match $flag {
			'-h' | '--help' => {
				print 'Usage: tg run .#release -- [--cargo] [--force] [--retry] [--no-sign] [--stage <compile|push|pull|package|publish>] [--target <target>] [--target-dir <path>] [--version <version>] [--build-number <number>]

Repeat --stage or --target to select more than one.
The default stage is package, or compile followed by package with --cargo.
The compile stage without --cargo builds the gnu and macOS targets through the compiler cache proxy, and a Linux machine builds all of them.
Cargo reuses its configured target directory, including CARGO_TARGET_DIR. Override it with --target-dir. Use --retry to retry failed toolchain and compiler-cache builds.
It also stages the provider library for each macOS target, and packaging on a Mac bundles the staged helper and library instead of building them with Cargo.
The push stage tags the staged files of each target as tangram/release/<commit>/<target> and pushes the tags to the default remote. The <commit> is the commit of a clean checkout, or a hash of the edited contents.
The pull stage pulls the tags for the commit of this checkout from the default remote and stages their files in release/, replacing the files that are staged. Without --target it pulls every target that is tagged.
A push does not replace a tag that has a different target unless --force is passed.
Each staged target includes its source revision and file checksums. Push and package reject stale or changed files, and macOS targets require both the helper and its matching provider library.
The package stage signs Linux repositories using the authenticated cf CLI.
The package stage builds a container image for each GNU Linux target with Docker Buildx, and saves it under release/image/.
Signed macOS packaging requires a Mac and a positive --build-number, and includes Tangram.dmg and its Sparkle appcast.
The package stage signs and notarizes the macOS binaries when TANGRAM_APPLE_P12, TANGRAM_APPLE_P12_PASSWORD_FILE, and TANGRAM_APPLE_API_KEY are set.
Downloaded keys are stored temporarily under keys/. Use --no-sign for local unsigned packages.
Publishing requires --version, a matching GitHub tag, and signed packaging for the selected targets.
Existing assets can be replaced, including for version 0.0.0.'
				return
			}
			'--cargo' => { $cargo = true }
			'--force' => { $force = true }
			'--retry' => { $retry = true }
			'--target-dir' => {
				let value = $remaining | get -o 0 | default ''
				if ($value | is-empty) or ($value | str starts-with '--') {
					error make {msg: 'expected --target-dir followed by a path'}
				}
				$env.CARGO_TARGET_DIR = $value | path expand
				$remaining = $remaining | skip 1
			}
			'--build-number' => {
				let value = $remaining | get -o 0 | default ''
				if $value !~ '^[1-9][0-9]*$' {
					error make {msg: 'expected --build-number followed by a positive integer'}
				}
				$build_number = $value
				$remaining = $remaining | skip 1
			}
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
				compile => { compile $cargo $selected_targets $retry }
				push => { push $selected_targets $force }
				pull => { pull $selected_targets }
				package => { package $selected_targets $version $sign $build_number }
				publish => { ^python3 scripts/release/publish.py publish $version ...$selected_targets }
			}
		}
	}
}

def compile [cargo: bool, selected_targets: list<string>, retry: bool] {
	if not $cargo {
		compile-tangram $selected_targets $retry
		return
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

	let commit = ^python3 scripts/version.py | str trim
	let provider_args = if ($target ends-with '-darwin') { ['-p' 'tangram_vfs_provider'] } else { [] }
	with-env {TANGRAM_CLI_COMMIT_HASH: $commit} {
		^cargo build --release -p tangram_cli ...$provider_args
	}
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
		let staged = $temporary | path join ($destination | path basename)
		^install -m 755 $binary $staged
		if ($target ends-with '-darwin') {
			^install -m 644 ($directory | path join release libtangram_vfs_provider.a) ($temporary | path join $'libtangram_vfs_provider_($target).a')
		}
		assert-source $commit
		stage record $temporary $target $commit
		stage install $temporary $target $commit
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
	print $'staged ($destination)'
}

def compile-tangram [selected_targets: list<string>, retry: bool] {
	let unsupported = $selected_targets | where {|target| $target not-in $tangram_targets }
	if not ($unsupported | is-empty) {
		error make {msg: $'Tangram does not build ($unsupported | str join ", "), so build on that platform with --cargo'}
	}
	let selected_targets = if ($selected_targets | is-empty) {
		$tangram_targets | where {|target| $nu.os-info.name != 'macos' or ($target ends-with '-darwin') }
	} else {
		$selected_targets
	}
	if $nu.os-info.name == 'macos' and ($selected_targets | any {|target| $target !~ '-darwin$' }) {
		error make {msg: 'a macOS machine cannot build Linux release targets'}
	}
	let commit = ^python3 scripts/version.py | str trim
	let directory = ^cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let retry_flags = if $retry { ['--retry'] } else { [] }

	mkdir release
	let temporary = ^mktemp -d release/.compile.XXXXXXXX | str trim
	try {
		for target in $selected_targets {
			let host = $target | str replace '-darwin' '-apple-darwin' | str replace '-linux-' '-unknown-linux-'
			let arg = {commit: $commit, host: $host}

			# Build the command with retry enabled before running Cargo in the user's workspace and target directory.
			let command = ^tg build ./build.tg.ts#release ...$retry_flags --arg-value ($arg | to json --raw) | str trim
			with-env {TGRUSTC_RETRY: (if $retry { '1' } else { '0' })} {
				^tg run ...$retry_flags $command
			}

			# Export the executable without changing the Cargo output or its fingerprints.
			let output = $temporary | path join $target
			mkdir $output
			let binary = $directory | path join $host release tangram
			let id = ^tg checkin --no-tokens $binary | str trim | split row '?' | first
			^tg build ./build.tg.ts#releaseBinary ...$retry_flags --arg-value $id --arg-value ($host | to json --raw) $'--checkout=($output | path join $"tangram_($target)")' | ignore
			if ($target ends-with '-darwin') {
				^install -m 644 ($directory | path join $host release libtangram_vfs_provider.a) ($output | path join $'libtangram_vfs_provider_($target).a')
			}
			assert-source $commit
			stage record $output $target $commit
			stage install $output $target $commit
			print $'staged ($target)'
		}
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}

def push [selected_targets: list<string>, force: bool] {
	let selected_targets = if ($selected_targets | is-empty) {
		$targets | where {|target| (staged-binary $target | path type) == file }
	} else {
		$selected_targets
	}
	if ($selected_targets | is-empty) {
		error make {msg: 'no Tangram binaries are staged in release/'}
	}
	let commit = ^python3 scripts/version.py | str trim
	for target in $selected_targets {
		stage validate release $target $commit
	}
	let force_flags = if $force { ['--force'] } else { [] }
	let temporary = ^mktemp -d release/.push.XXXXXXXX | str trim
	try {
		for target in $selected_targets {
			# Collect the staged files of the target in a directory.
			let directory = $temporary | path join $target
			mkdir $directory
			for file in (stage files $target | append {mode: '644', name: (stage metadata $target)}) {
				^install -m $file.mode ('release' | path join $file.name) ($directory | path join $file.name)
			}
			stage validate $directory $target $commit

			# Tag the directory and push the tag.
			let tag = release-tag $commit $target
			let id = ^tg checkin --no-tokens $directory | str trim | split row '?' | first
			^tg tag put --create-ancestors ...$force_flags $tag $id
			^tg push ...$force_flags $tag
			print $'pushed ($tag) ($id)'
		}
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}

def pull [selected_targets: list<string>] {
	let commit = ^python3 scripts/version.py | str trim
	let selected_targets = if ($selected_targets | is-empty) { tagged-targets $commit } else { $selected_targets }
	mkdir release
	let temporary = ^mktemp -d release/.pull.XXXXXXXX | str trim
	try {
		for target in $selected_targets {
			let tag = release-tag $commit $target

			# Pull the tag, and replace a local tag that has a different target, because the remote decides what the tag is.
			let output = ^tg pull --force $tag | complete
			if $output.exit_code != 0 {
				error make {msg: $'failed to pull ($tag): ($output.stderr | str trim)'}
			}

			# Stage the files of the tag.
			let directory = $temporary | path join $target
			^tg checkout --no-lock $tag $'--path=($directory | path expand)' | ignore
			stage install $directory $target $commit
			print $'staged ($target)'
		}
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	^chmod -R u+w $temporary
	rm -rf $temporary
}

# List the targets that are tagged on the remote for a commit. Do not use a cached listing, which can omit a target and hide a remote that cannot be reached, because a pull of a tag fails in the same way when the tag does not exist and when the remote cannot be reached.
def tagged-targets [commit: string] {
	let parent = $'($tag_prefix)/($commit)'
	let output = ^tg list --all --remote --ttl 0 $parent | complete
	if $output.exit_code != 0 {
		error make {msg: $'failed to list the release targets that are tagged under ($parent): ($output.stderr | str trim)'}
	}
	let names = $output.stdout | from json | get specifier | each {|specifier| $specifier | split row '/' | last }
	let tagged = $targets | where {|target| $target in $names }
	if ($tagged | is-empty) {
		error make {msg: $'no release targets are tagged under ($parent)'}
	}
	$tagged
}

def package [selected_targets: list<string>, version: string, sign: bool, build_number: string] {
	let selected_targets = if ($selected_targets | is-empty) {
		$targets | where {|target| (staged-binary $target | path type) == file }
	} else {
		$selected_targets
	}
	if ($selected_targets | is-empty) {
		error make {msg: 'no Tangram binaries are staged in release/'}
	}
	let commit = ^python3 scripts/version.py | str trim
	for target in $selected_targets {
		stage validate release $target $commit
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

	# Check before packaging that this machine runs containers for the architecture of each container image, because its Dockerfile runs commands in the image.
	for target in ($selected_targets | where {|target| $target ends-with '-linux-gnu' }) {
		let platform = if ($target starts-with 'aarch64-') { 'linux/arm64' } else { 'linux/amd64' }
		let output = ^docker run --rm --platform $platform debian:trixie-slim true | complete
		if $output.exit_code != 0 {
			error make {msg: $'this machine cannot run ($platform) containers, which the container image for ($target) needs, so install QEMU emulation or package it on another machine: ($output.stderr | str trim)'}
		}
	}
	if $macos_app {
		let sign_flags = if $sign { [] } else { ['--no-sign'] }
		let build_flags = if ($build_number | is-empty) { [] } else { ['--build-number' $build_number] }

		# Bundle a matching helper and provider library for this Mac's architecture.
		let arch = ^uname -m | str trim | str replace 'arm64' 'aarch64'
		let target = $'($arch)-darwin'
		stage validate release $target $commit
		let helper = staged-binary $target
		let provider = $'release/libtangram_vfs_provider_($arch)-darwin.a'
		^nu scripts/macos/release.nu --version $version ...$sign_flags ...$build_flags --helper $helper --provider $provider
	}
	let manifest = ('release/manifest.json' | path expand)
	if ($manifest | path type) == file {
		rm $manifest
	}
	if $macos_app {
		let staged = ^mktemp -d release/.macos.XXXXXXXX | str trim
		try {
			let names = if $sign { ['Tangram.dmg' 'appcast.xml'] } else { ['Tangram.dmg'] }
			for name in $names {
				let source = ('packages/macos/build/Distribution' | path join $name)
				let destination = ($staged | path join $name)
				^install -m 644 $source $destination
			}
			for name in $names {
				place-file ($staged | path join $name) ('release' | path join $name)
			}
		} catch {|error|
			rm -r $staged
			error make $error.raw
		}
		rm -r $staged
	}

	# Sign copies so that packaging preserves the compiled files and their provenance.
	let temporary = ^mktemp -d release/.binaries.XXXXXXXX | str trim | path expand
	try {
		let darwin_targets = $selected_targets | where {|target| $target ends-with '-darwin' }
		for target in $darwin_targets {
			^install -m 755 (staged-binary $target) ($temporary | path join $'tangram_($target)')
		}
		if $sign {
			^nu scripts/release/sign.nu --directory $temporary ...$darwin_targets
		}
		package-binaries $selected_targets $version $sign $temporary
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}

def package-binaries [selected_targets: list<string>, version: string, sign: bool, directory: path] {
	for target in $selected_targets {
		let binary = if ($target ends-with '-darwin') {
			$directory | path join $'tangram_($target)'
		} else {
			staged-binary $target
		}
		let archive = $'(staged-binary $target).tar.gz'
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
				^nu packages/linux/image/package.nu $archive $architectures.deb $version
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

# Reject source edits made while Cargo was compiling the release.
def assert-source [commit: string] {
	if (^python3 scripts/version.py | str trim) != $commit {
		error make {msg: 'the source changed during compilation; compile the target again'}
	}
}

def release-tag [commit: string, target: string] {
	$'($tag_prefix)/($commit)/($target)'
}

def place-file [source: path, destination: path] {
	if ($destination | path type) == dir {
		error make {msg: $'the release destination is a directory ($destination)'}
	}
	mv -f $source $destination
}
