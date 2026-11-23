#!/usr/bin/env nu

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
				print 'Usage: tg run .#release -- [--cargo] [--force] [--no-sign] [--stage <compile|push|pull|package|publish>] [--target <target>] [--version <version>] [--build-number <number>]

Repeat --stage or --target to select more than one.
The default stage is package, or compile followed by package with --cargo.
The compile stage without --cargo builds the gnu and macOS targets with Tangram, and a Linux machine builds all of them.
It also stages the provider library for each macOS target, and packaging on a Mac bundles the staged helper and library instead of building them with Cargo.
The push stage tags the staged files of each target as tangram/release/<commit>/<target> and pushes the tags to the default remote. The <commit> is the commit of a clean checkout, or a hash of the edited contents.
The pull stage pulls the tags for the commit of this checkout from the default remote and stages their files in release/, replacing the files that are staged. Without --target it pulls every target that is tagged.
A push does not replace a tag that has a different target unless --force is passed.
The package stage signs Linux repositories using the authenticated cf CLI.
Signed macOS packaging requires a Mac and a positive --build-number, and includes Tangram.dmg and its Sparkle appcast.
The package stage signs and notarizes the macOS binaries when TANGRAM_APPLE_P12, TANGRAM_APPLE_P12_PASSWORD_FILE, and TANGRAM_APPLE_API_KEY are set.
Downloaded keys are stored temporarily under keys/. Use --no-sign for local unsigned packages.
Publishing requires --version, a matching GitHub tag, and signed packaging for the selected targets.
Existing assets can be replaced, including for version 0.0.0.'
				return
			}
			'--cargo' => { $cargo = true }
			'--force' => { $force = true }
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
				compile => { compile $cargo $selected_targets }
				push => { push $selected_targets $force }
				pull => { pull $selected_targets }
				package => { package $selected_targets $version $sign $build_number }
				publish => { ^python3 scripts/release/publish.py publish $version ...$selected_targets }
			}
		}
	}
}

def compile [cargo: bool, selected_targets: list<string>] {
	if not $cargo {
		compile-tangram $selected_targets
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

def compile-tangram [selected_targets: list<string>] {
	let unsupported = $selected_targets | where {|target| $target not-in $tangram_targets }
	if not ($unsupported | is-empty) {
		error make {msg: $'Tangram does not build ($unsupported | str join ", "), so build on that platform with --cargo'}
	}
	let commit = ^python3 scripts/version.py | str trim
	let arg = if ($selected_targets | is-empty) { {commit: $commit} } else { {commit: $commit, targets: $selected_targets} }

	mkdir release
	let temporary = ^mktemp -d release/.compile.XXXXXXXX | str trim
	try {
		let output = $temporary | path join output
		^tg build ./build.tg.ts#release --arg-value ($arg | to json --raw) $'--checkout=($output)'
		for file in (ls $output | get name) {
			let destination = 'release' | path join ($file | path basename)
			let staged = $temporary | path join staged
			let mode = if ($file | str ends-with '.a') { '644' } else { '755' }
			^install -m $mode $file $staged
			place-file $staged $destination
			print $'staged ($destination)'
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
	let force_flags = if $force { ['--force'] } else { [] }
	let temporary = ^mktemp -d release/.push.XXXXXXXX | str trim
	try {
		for target in $selected_targets {
			# Collect the staged files of the target in a directory.
			let directory = $temporary | path join $target
			mkdir $directory
			for file in (staged-files $target) {
				if $file.required and ($file.path | path type) != file {
					error make {msg: $'expected a staged executable at ($file.path)'}
				}
				if ($file.path | path type) == file {
					^install -m $file.mode $file.path ($directory | path join ($file.path | path basename))
				}
			}

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
	let candidates = if ($selected_targets | is-empty) { $targets } else { $selected_targets }
	mkdir release
	let temporary = ^mktemp -d release/.pull.XXXXXXXX | str trim
	let pulled = try {
		$candidates | each {|target|
			let tag = release-tag $commit $target

			# Pull the tag, and replace a local tag that has a different target, because the remote decides what the tag is.
			let output = ^tg pull --force $tag | complete
			if $output.exit_code != 0 {
				# Skip a target that is not tagged unless it was selected.
				if ($selected_targets | is-empty) and ($output.stderr | str contains 'failed to get the reference') {
					return null
				}
				error make {msg: $'failed to pull ($tag): ($output.stderr | str trim)'}
			}

			# Stage the files of the tag.
			let directory = $temporary | path join $target
			^tg checkout --no-lock $tag $'--path=($directory | path expand)' | ignore
			let names = ls $directory | get name | each {|name| $name | path basename }
			for file in (staged-files $target) {
				let name = $file.path | path basename
				if $name not-in $names {
					if $file.required {
						error make {msg: $'($tag) does not contain ($name)'}
					}
					continue
				}
				let staged = $temporary | path join staged
				^install -m $file.mode ($directory | path join $name) $staged
				place-file $staged $file.path
				print $'staged ($file.path)'
			}
			$target
		} | compact
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	^chmod -R u+w $temporary
	rm -rf $temporary
	if ($pulled | is-empty) {
		error make {msg: $'no release targets are tagged under ($tag_prefix)/($commit)'}
	}
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
		let build_flags = if ($build_number | is-empty) { [] } else { ['--build-number' $build_number] }

		# Bundle the staged helper and provider library for this Mac's architecture when both are staged, so that the app needs no Rust build.
		let arch = ^uname -m | str trim | str replace 'arm64' 'aarch64'
		let helper = staged-binary $'($arch)-darwin'
		let provider = $'release/libtangram_vfs_provider_($arch)-darwin.a'
		let prebuilt_flags = if ($helper | path type) == file and ($provider | path type) == file {
			['--helper' $helper '--provider' $provider]
		} else {
			[]
		}
		^nu scripts/macos/release.nu --version $version ...$sign_flags ...$build_flags ...$prebuilt_flags
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
	if $sign {
		let darwin_targets = $selected_targets | where {|target| $target ends-with '-darwin' }
		^nu scripts/release/sign.nu ...$darwin_targets
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

# List the files that the compile stage stages for a target.
def staged-files [target: string] {
	let binary = {mode: '755', path: (staged-binary $target), required: true}
	if ($target ends-with '-darwin') {
		[$binary {mode: '644', path: $'release/libtangram_vfs_provider_($target).a', required: false}]
	} else {
		[$binary]
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
