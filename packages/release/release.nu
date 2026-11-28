#!/usr/bin/env nu

use ./stage.nu

# The module of the sandboxed packaging builds.
const builds = './packages/release/build.tg.ts'

# Run one step of the release for one target. The default export of packages/release/tangram.ts selects the targets and runs these steps.
def main [] {
	print 'Usage: tg run .#release -- --help'
}

# Compile a release target with Cargo on the host, in the user's target directory, where the rustc proxy compiles each crate in a sandbox, and stage it in release/ with the artifact of its files.
def 'main compile' [
	target: string # The release target to compile.
	--retry # Retry failed builds.
] {
	let commit = ^python3 scripts/version.py | str trim
	let retry_flags = if $retry { ['--retry'] } else { [] }
	let arg = {commit: $commit, target: $target}
	let host = $target | str replace --regex '-static$' '' | str replace '-darwin' '-apple-darwin' | str replace '-linux-' '-unknown-linux-'

	# Give the static musl target its own target directory, because it shares its Rust target with the dynamic musl target and differs only in its flags.
	let directory = $env.CARGO_TARGET_DIR? | default target | path expand
	let directory = if ($target ends-with '-static') { $directory | path join static } else { $directory }
	mkdir release
	let temporary = ^mktemp -d release/.compile.XXXXXXXX | str trim
	try {
		# Create the command, and run Cargo.
		let command = ^tg build ./build.tg.ts#release ...$retry_flags --arg-value ($arg | to json --raw) | str trim
		with-env {CARGO_TARGET_DIR: $directory, TGRUSTC_RETRY: (if $retry { '1' } else { '0' })} {
			^tg run ...$retry_flags $command
		}

		# Collect the executable, and the provider library for a macOS target, and check them out.
		let outputs = $directory | path join $host release
		let files = {executable: (checkin ($outputs | path join tangram))}
		let files = if ($target ends-with '-darwin') {
			$files | insert provider (checkin ($outputs | path join libtangram_vfs_provider.a))
		} else {
			$files
		}
		let artifact = ^tg build $'($builds)#binary' ...$retry_flags --arg-value (arg-value {target: $target} $files) | artifact-id
		let output = $temporary | path join output
		^tg checkout $artifact --path $output
		^install -m 755 ($output | path join tangram) ($temporary | path join $'tangram_($target)')
		let provider = $output | path join libtangram_vfs_provider.a
		if ($target ends-with '-darwin') and ($provider | path type) == file {
			^install -m 644 $provider ($temporary | path join $'libtangram_vfs_provider_($target).a')
		}

		# Record the provenance of the files, which requires every file of the target, and stage them.
		assert-source $commit
		stage record $temporary $target $commit $artifact
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

# Check a target's staged files against the source, and archive its binary with the tg symlink and the tgx wrapper in a sandboxed build at release/tangram_<target>.tar.gz. Sign and notarize a copy of a macOS binary first unless --no-sign is passed, and archive the signed copy.
def 'main tarball' [
	target: string # The release target to package.
	--no-sign # Package a macOS binary without signing it.
	--retry # Retry failed builds.
] {
	let release = staged-artifact $target
	let binary = staged-binary $target
	if ($binary | path type) != file or (^test -x $binary | complete).exit_code != 0 {
		error make {msg: $'expected a staged executable at ($binary)'}
	}
	let temporary = ^mktemp -d release/.package.XXXXXXXX | str trim | path expand
	try {
		# Sign a copy outside the sandbox so that packaging preserves the compiled files and their provenance, and archive the copy.
		let release = if ($target ends-with '-darwin') and not $no_sign {
			let copy = $temporary | path join $'tangram_($target)'
			^install -m 755 $binary $copy
			^nu packages/release/sign.nu --directory $temporary $target
			let directory = $temporary | path join signed
			mkdir $directory
			mv $copy ($directory | path join tangram)
			checkin $directory
		} else {
			$release
		}
		let arg = arg-value {sourceDateEpoch: (source-date-epoch)} {release: $release}
		build-file tarball $arg $'($binary).tar.gz' $retry
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
	print $'packaged ($binary).tar.gz'
}

# Build the Debian package of a GNU Linux target in a sandboxed build, and place it in release/apt/.
def 'main deb' [
	target: string # The GNU Linux release target to package.
	--retry # Retry failed builds.
	--version: string # The version of the package.
] {
	build-package deb $target $version release/apt $retry
}

# Build the unsigned RPM package of a GNU Linux target in a sandboxed build, and place it in release/rpm/.
def 'main rpm' [
	target: string # The GNU Linux release target to package.
	--retry # Retry failed builds.
	--version: string # The version of the package.
] {
	build-package rpm $target $version release/rpm $retry
}

# Build the unsigned Alpine package of a musl Linux target in a sandboxed build, and place it in release/apk/<architecture>/.
def 'main apk' [
	target: string # The musl Linux release target to package.
	--retry # Retry failed builds.
	--version: string # The version of the package.
] {
	let architecture = $target | split row '-' | first
	build-package apk $target $version ('release/apk' | path join $architecture) $retry
}

# Prepare the AUR source package for the packaged GNU Linux tarballs in a sandboxed build, and place it in release/aur/.
def 'main aur' [
	version: string # The version of the release.
	...targets: string # The GNU Linux release targets whose tarballs the package downloads.
	--retry # Retry failed builds.
] {
	if ($targets | is-empty) or ($targets | any {|target| $target not-in [aarch64-linux-gnu x86_64-linux-gnu] }) {
		error make {msg: 'expected GNU Linux targets for the AUR package'}
	}
	let archives = $targets | each {|target|
		{($target | split row '-' | first): (open --raw $'release/tangram_($target).tar.gz' | hash sha256)}
	} | reduce -f {} {|entry, archives| $archives | merge $entry }
	let arg = {archives: $archives, version: $version} | to json --raw
	build-directory aur $arg release/aur $retry
	print 'prepared release/aur'
}

# Generate the unsigned repository metadata over the packages of the targets in release/ in a sandboxed build: the apt Packages and Release files, the rpm repodata, and the APKINDEX of each architecture. Run it after signing the packages, because the rpm and apk metadata cover the signed packages.
def 'main repository' [
	version: string # The version of the release.
	...targets: string # The release targets whose packages to index.
	--retry # Retry failed builds.
] {
	# Collect the packages of each repository.
	let architectures = [aarch64 x86_64]
	let gnu = $architectures | where {|architecture| $'($architecture)-linux-gnu' in $targets }
	let musl = $architectures | where {|architecture| $'($architecture)-linux-musl' in $targets }
	let repositories = {
		apt: ($gnu | each {|architecture| $'apt/(deb-name $version $architecture)' })
		rpm: ($gnu | each {|architecture| $'rpm/(rpm-name $version $architecture)' })
		apk: ($musl | each {|architecture| $'apk/($architecture)/(apk-name $version)' })
	} | transpose name files | where {|repository| $repository.files | is-not-empty }
	if ($repositories | is-empty) {
		return
	}

	# Check in a copy of each repository's packages, so that the metadata covers exactly these files.
	let temporary = ^mktemp -d release/.repository.XXXXXXXX | str trim | path expand
	try {
		let artifacts = $repositories | each {|repository|
			for file in $repository.files {
				let source = 'release' | path join $file
				if ($source | path type) != file {
					error make {msg: $'expected a package at ($source)'}
				}
				let destination = $temporary | path join packages $file
				mkdir ($destination | path dirname)
				^cp $source $destination
			}
			{($repository.name): (checkin ($temporary | path join packages $repository.name))}
		} | reduce -f {} {|entry, artifacts| $artifacts | merge $entry }

		# Build the metadata, and place it beside the packages, replacing the rpm repodata as a whole and removing an earlier signature of the apt release file.
		let output = $temporary | path join output
		let arg = arg-value {sourceDateEpoch: (source-date-epoch)} $artifacts
		build $'($builds)#repository' $arg $output $retry
		if ($output | path join apt | path type) == dir {
			for name in [Packages Packages.gz Release] {
				place-file ($output | path join apt $name) ('release/apt' | path join $name)
			}
			rm -f release/apt/InRelease
		}
		if ($output | path join rpm | path type) == dir {
			let previous = $temporary | path join previous-repodata
			if ('release/rpm/repodata' | path exists) {
				mv release/rpm/repodata $previous
			}
			mv ($output | path join rpm repodata) release/rpm/repodata
		}
		for architecture in $musl {
			place-file ($output | path join apk $architecture APKINDEX.tar.gz) ('release/apk' | path join $architecture APKINDEX.tar.gz)
		}
	} catch {|error|
		^chmod -R u+w $temporary
		rm -rf $temporary
		error make $error.raw
	}
	^chmod -R u+w $temporary
	rm -rf $temporary
	print $'generated the ($repositories | get name | str join ", ") repository metadata'
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

# Build a target's package in a sandboxed build from its compiled artifact, and place each file of the output in the destination directory.
def build-package [name: string, target: string, version: any, destination: path, retry: bool] {
	if $version == null {
		error make {msg: 'expected --version followed by a package version'}
	}
	let release = staged-artifact $target
	let arg = arg-value {sourceDateEpoch: (source-date-epoch), target: $target, version: $version} {release: $release}
	build-directory $name $arg $destination $retry
}

# Build a file with an export of the packaging builds, and place it at the destination.
def build-file [name: string, arg: string, destination: path, retry: bool] {
	let temporary = ^mktemp -d release/.build.XXXXXXXX | str trim
	try {
		let output = $temporary | path join output
		build $'($builds)#($name)' $arg $output $retry
		place-file $output $destination
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}

# Build a directory with an export of the packaging builds, and place each of its files in the destination directory.
def build-directory [name: string, arg: string, destination: path, retry: bool] {
	let temporary = ^mktemp -d release/.build.XXXXXXXX | str trim
	try {
		let output = $temporary | path join output
		build $'($builds)#($name)' $arg $output $retry
		mkdir $destination
		for file in (ls --all $output | get name) {
			let placed = $destination | path join ($file | path basename)
			place-file $file $placed
			print $'packaged ($placed)'
		}
	} catch {|error|
		rm -rf $temporary
		error make $error.raw
	}
	rm -rf $temporary
}

# Run a sandboxed Tangram build and check out its output.
def build [reference: string, arg: string, output: path, retry: bool] {
	let retry_flags = if $retry { ['--retry'] } else { [] }
	^tg build $reference ...$retry_flags --arg-value $arg $'--checkout=($output)'
}

# Check in a file or directory and get its artifact.
def checkin [path: path] {
	^tg checkin --root --no-lock $path | artifact-id
}

# Get the artifact ID from a printed reference, without its options and tokens.
def artifact-id [] {
	let id = $in | str trim | split row '?' | first
	if $id !~ '^(dir|fil)_[0-9a-z]+$' {
		error make {msg: $'expected an artifact, got ($id)'}
	}
	$id
}

# Write a build arg as a Tangram value, a map of the JSON values and the artifacts.
def arg-value [values: record, artifacts: record] {
	let values = $values | transpose key value | each {|entry| $'($entry.key | to json):($entry.value | to json --raw)' }
	let artifacts = $artifacts | transpose key value | each {|entry| $'($entry.key | to json):($entry.value | artifact-id)' }
	$'{($values | append $artifacts | str join ",")}'
}

# Get the time of the source revision's commit, which the packages give their files.
def source-date-epoch [] {
	^git log -1 --format=%ct | str trim | into int
}

# Check a target's staged files against the source, and get its compiled artifact.
def staged-artifact [target: string] {
	let commit = ^python3 scripts/version.py | str trim
	stage validate release $target $commit
	stage artifact release $target
}

def staged-binary [target: string] {
	$'release/tangram_($target)'
}

def deb-name [version: string, architecture: string] {
	let architecture = if $architecture == aarch64 { 'arm64' } else { 'amd64' }
	$'tangram_($version)_($architecture).deb'
}

def rpm-name [version: string, architecture: string] {
	$'tangram-($version | str replace --all '-' '~')-1.($architecture).rpm'
}

def apk-name [version: string] {
	let version = $version | str replace --regex '-(alpha|beta|pre|rc)\.?([0-9]*)$' '_${1}${2}'
	$'tangramdotdev-($version)-r0.apk'
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
