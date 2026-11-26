#!/usr/bin/env nu

use ./stage.nu

# Run one step of the release for one target. The default export of packages/release/tangram.ts selects the targets and runs these steps.
def main [] {
	print 'Usage: tg run .#release -- --help'
}

# Compile a release target in a sandboxed Tangram build, and stage it in release/.
def 'main compile' [
	target: string # The release target to compile.
	--retry # Retry failed builds.
] {
	let commit = ^python3 scripts/version.py | str trim
	let retry_flags = if $retry { ['--retry'] } else { [] }
	let arg = {commit: $commit, target: $target}
	mkdir release
	let temporary = ^mktemp -d release/.compile.XXXXXXXX | str trim
	try {
		# Build the standalone executable, and the provider library for a macOS target, and check them out.
		let output = $temporary | path join output
		^tg build ./build.tg.ts#release ...$retry_flags --arg-value ($arg | to json --raw) $'--checkout=($output)'
		^install -m 755 ($output | path join tangram) ($temporary | path join $'tangram_($target)')
		let provider = $output | path join libtangram_vfs_provider.a
		if ($target ends-with '-darwin') and ($provider | path type) == file {
			^install -m 644 $provider ($temporary | path join $'libtangram_vfs_provider_($target).a')
		}

		# Record the provenance of the files, which requires every file of the target, and stage them.
		assert-source $commit
		stage record $temporary $target $commit
		stage install $temporary $target $commit
	} catch {|error|
		^chmod -R u+w $temporary
		rm -rf $temporary
		error make $error.raw
	}
	^chmod -R u+w $temporary
	rm -rf $temporary
	print $'staged ($target)'
}

# Check a target's staged files against the source, and package its binary with the tg symlink and the tgx wrapper in release/tangram_<target>.tar.gz. Sign and notarize a copy of a macOS binary first unless --no-sign is passed.
def 'main tarball' [
	target: string # The release target to package.
	--no-sign # Package a macOS binary without signing it.
] {
	let commit = ^python3 scripts/version.py | str trim
	stage validate release $target $commit
	let binary = staged-binary $target
	if ($binary | path type) != file or (^test -x $binary | complete).exit_code != 0 {
		error make {msg: $'expected a staged executable at ($binary)'}
	}
	let archive = $'($binary).tar.gz'
	let temporary = ^mktemp -d release/.package.XXXXXXXX | str trim | path expand
	try {
		# Sign a copy so that packaging preserves the compiled files and their provenance.
		let binary = if ($target ends-with '-darwin') {
			let copy = $temporary | path join $'tangram_($target)'
			^install -m 755 $binary $copy
			if not $no_sign {
				^nu packages/release/sign.nu --directory $temporary $target
			}
			$copy
		} else {
			$binary
		}
		let output = $temporary | path join archive.tar.gz
		let contents = $temporary | path join contents
		mkdir $contents
		^ln $binary ($contents | path join tangram)
		^ln -s tangram ($contents | path join tg)
		let wrapper = $contents | path join tgx
		'#!/bin/sh
exec tg run -b "$@"
' | save $wrapper
		^chmod 755 $wrapper
		with-env { COPYFILE_DISABLE: '1' } {
			^tar --no-xattrs -czf $output -C $contents tangram tg tgx
		}
		place-file $output $archive
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
	print $'packaged ($archive)'
}

# Build the macOS app with the staged helper and provider library for this Mac's architecture, and place Tangram.dmg, and its Sparkle appcast when signing, in release/.
def 'main macos' [
	--build-number: string # The build number of the app.
	--no-sign # Build the app without signing it.
	--version: string # The version of the app.
] {
	# Bundle a matching helper and provider library for this Mac's architecture.
	let commit = ^python3 scripts/version.py | str trim
	let arch = ^uname -m | str trim | str replace 'arm64' 'aarch64'
	let target = $'($arch)-darwin'
	stage validate release $target $commit
	let helper = staged-binary $target
	let provider = $'release/libtangram_vfs_provider_($target).a'
	let sign_flags = if $no_sign { ['--no-sign'] } else { [] }
	let build_flags = if ($build_number | is-empty) { [] } else { ['--build-number' $build_number] }
	^nu scripts/macos/release.nu --version $version ...$sign_flags ...$build_flags --helper $helper --provider $provider

	# Copy the files before replacing any of them.
	let staged = ^mktemp -d release/.macos.XXXXXXXX | str trim
	try {
		let names = if $no_sign { ['Tangram.dmg'] } else { ['Tangram.dmg' 'appcast.xml'] }
		for name in $names {
			^install -m 644 ('packages/macos/build/Distribution' | path join $name) ($staged | path join $name)
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

def staged-binary [target: string] {
	$'release/tangram_($target)'
}

# Reject source edits made while the release was compiling.
def assert-source [commit: string] {
	if (^python3 scripts/version.py | str trim) != $commit {
		error make {msg: 'the source changed during compilation; compile the target again'}
	}
}

def place-file [source: path, destination: path] {
	if ($destination | path type) == dir {
		error make {msg: $'the release destination is a directory ($destination)'}
	}
	mv -f $source $destination
}
