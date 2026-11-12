#!/usr/bin/env nu

use ./common.nu *

const image = 'tangram-release-builder'

# Build the release archive for one target.
def main [
	--notary-profile: string # The notarytool keychain profile. Defaults to $TANGRAM_NOTARY_PROFILE, then to Tangram.
	--skip-signing # On macOS, leave the binary with the linker's ad hoc signature and do not notarize it.
	--target: string # The Rust target triple. Defaults to the target of this machine.
] {
	# Resolve the target.
	let target = $target | default (host_target)
	let entry = $targets | get --optional $target
	if $entry == null {
		error make { msg: $'unsupported target: ($target)', help: $'The targets are ($targets | columns | str join ", ").' }
	}
	if $entry.os == 'darwin' and (host_os) != 'darwin' {
		error make { msg: $'the target ($target) must be built on macOS' }
	}
	if $entry.os == 'linux' {
		let arch = ^docker info --format '{{.Architecture}}' | str trim
		if $arch != $entry.arch {
			error make { msg: $'the target ($target) must be built natively, but Docker runs ($arch) containers' }
		}
	}

	# Describe the build.
	let root = repository
	cd $root
	let version = version
	let commit = commit
	let release_path = $root | path join release
	let stage_path = $release_path | path join $'tangram_($entry.host)'
	let archive_path = $release_path | path join $'tangram_($entry.host).tar.gz'
	print -e $'building tangram ($version)+($commit) for ($target)'

	# Create an empty stage.
	rm -rf $stage_path $archive_path
	mkdir $stage_path
	let binary_path = $stage_path | path join tangram

	# Build the binary.
	match $entry.os {
		'darwin' => { build_darwin $target $commit $binary_path },
		'linux' => { build_linux $target $commit $stage_path },
	}

	# Sign and notarize the binary.
	if $entry.os == 'darwin' and not $skip_signing {
		let profile = $notary_profile | default ($env.TANGRAM_NOTARY_PROFILE? | default Tangram)
		sign_darwin $binary_path $profile
	}

	# Create the archive.
	step 'Creating the archive'
	^ln -sfn tangram ($stage_path | path join tg)
	"#!/bin/sh\nexec tg run -b \"$@\"\n" | save --force ($stage_path | path join tgx)
	^chmod 755 ($stage_path | path join tgx)
	with-env { COPYFILE_DISABLE: '1' } {
		^tar -czf $archive_path -C $stage_path tangram tg tgx
	}
	rm -rf $stage_path

	let size = ls $archive_path | get size | first
	print -e $'wrote ($archive_path) [($size)]'
}

# Build the binary for a macOS target on this machine.
def build_darwin [target: string, commit: string, binary_path: path] {
	step $'Building for ($target)'
	let installed = ^rustup target list --installed | lines
	if $target not-in $installed {
		error make { msg: $'the Rust target ($target) is not installed', help: $'Run `rustup target add ($target)`.' }
	}
	let env_ = {
		MACOSX_DEPLOYMENT_TARGET: (deployment_target),
		TANGRAM_CLI_COMMIT_HASH: $commit,
	}
	with-env $env_ {
		^cargo build --release --locked --package tangram_cli --bin tangram --target $target
	}
	let target_path = ^cargo metadata --format-version 1 --no-deps | from json | get target_directory
	cp ($target_path | path join $target release tangram) $binary_path
}

# Build the binary for a Linux target in the builder container, from a copy of the files in the checkout that are not ignored.
def build_linux [target: string, commit: string, stage_path: path] {
	step 'Building the builder image'
	^docker build --tag $image (repository | path join scripts/release)

	step $'Building for ($target)'
	let files_path = mktemp --tmpdir tangram-release-files.XXXXXX
	^git ls-files --cached --others --exclude-standard
	| lines
	| where { |file| ($file | path type) in ['file' 'symlink'] }
	| str join "\n"
	| save --force $files_path
	let script = [
		'set -eu'
		'tar -xf - -C /build'
		$'cargo build --release --locked --package tangram_cli --bin tangram --target ($target)'
		$'install -m 755 target/($target)/release/tangram /output/tangram'
		'chown "$OUTPUT_OWNER" /output/tangram'
	] | str join "\n"
	let owner = $'(^id -u | str trim):(^id -g | str trim)'
	(
		^tar -cf - -T $files_path
		| ^docker run --rm --interactive
			--env $'OUTPUT_OWNER=($owner)'
			--env $'TANGRAM_CLI_COMMIT_HASH=($commit)'
			--volume tangram-release-cargo-git:/usr/local/cargo/git
			--volume tangram-release-cargo-registry:/usr/local/cargo/registry
			--volume tangram-release-target:/build/target
			--volume $'($stage_path):/output'
			$image sh -c $script
	)
	rm $files_path
}

# Sign the binary with the Developer ID Application identity and the hardened runtime, then notarize it.
def sign_darwin [binary_path: path, profile: string] {
	step 'Signing with the Developer ID Application identity'
	let identities = ^security find-identity -v -p codesigning | complete
	if not ($identities.stdout | str contains 'Developer ID Application') {
		error make {
			msg: 'a Developer ID Application signing identity is not available'
			help: 'Create a Developer ID Application certificate for the Tangram Apple Developer account, or pass --skip-signing to build an unsigned archive.'
		}
	}
	let entitlements_path = repository | path join scripts/release/tangram.entitlements
	(
		^codesign --force --options runtime --timestamp
			--entitlements $entitlements_path
			--identifier dev.tangram.tangram
			--sign 'Developer ID Application'
			$binary_path
	)
	^codesign --verify --strict --verbose=2 $binary_path

	step 'Submitting for notarization'
	let zip_path = $'($binary_path).zip'
	^ditto -c -k $binary_path $zip_path
	let output = ^xcrun notarytool submit $zip_path --keychain-profile $profile --wait --output-format json | from json
	rm $zip_path
	if $output.status != 'Accepted' {
		error make {
			msg: $'the notarization was not accepted: ($output.status)'
			help: $'Run `xcrun notarytool log ($output.id) --keychain-profile ($profile)`.'
		}
	}
	print -e $'the notarization was accepted: ($output.id)'
}
