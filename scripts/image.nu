#!/usr/bin/env nu

const repository_path = path self '../'
const repository = 'ghcr.io/tangramdotdev/tangram'
const default_tag = 'ghcr.io/tangramdotdev/tangram:latest'
const platforms = {
	'linux/amd64': 'x86_64-unknown-linux-gnu'
	'linux/arm64': 'aarch64-unknown-linux-gnu'
}

# Build the container image with Tangram and load or publish it with Docker.
def main [] {
	print 'Usage: nu scripts/image.nu <build|package|publish> [--fast] [--platform <linux/amd64|linux/arm64>] [--tag <tag>] [--version <version>]'
}

# Build the image for one platform and load it into Docker under the tag.
def 'main build' [
	--fast # Compile each crate in its own cached process with the fast profile, so that a small change rebuilds quickly.
	--platform: string # The platform to build for. The default is this machine's platform.
	--tag: string = $default_tag # The tag to give the loaded image.
] {
	cd $repository_path
	let platform = $platform | default (host-platform)
	load $platform $tag $fast
}

# Build the image for one platform and save it under release/image/, with the name and the tag that the release script's Dockerfile image has.
def 'main package' [
	--platform: string # The platform to build for. The default is this machine's platform.
	--version: string # The version of the release.
] {
	cd $repository_path
	if $version == null {
		error make {msg: 'expected --version followed by a package version'}
	}
	let platform = $platform | default (host-platform)
	let architecture = $platform | split row '/' | last
	let tag = $'($repository):($version | str replace --all --regex "[^0-9A-Za-z_.-]" "-")-($architecture)'
	load $platform $tag false
	let destination = ('release/image' | path expand)
	mkdir $destination
	let temporary = ^mktemp -d ($destination | path join '.image.XXXXXXXX') | str trim
	try {
		let name = $'tangram_($version)_($architecture).tar'
		let archive = $temporary | path join $name
		^docker save --output $archive $tag
		^chmod 644 $archive
		let output = $destination | path join $name
		if ($output | path type) == dir {
			error make {msg: $'the release destination is a directory ($output)'}
		}
		mv -f $archive $output
		print $'packaged ($output) as ($tag)'
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
}

# Build the image for every platform, push each one, and publish an index of them under the tag.
def 'main publish' [
	--tag: string = $default_tag # The tag of the index.
] {
	cd $repository_path
	let tags = $platforms | columns | each {|platform|
		let architecture = $platform | split row '/' | last
		let platform_tag = $'($tag)-($architecture)'
		load $platform $platform_tag false
		^docker push $platform_tag
		$platform_tag
	}
	^docker buildx imagetools create --tag $tag ...$tags
}

# Build the image for a platform and load it into Docker under a tag.
def load [platform: string, tag: string, fast: bool] {
	if $nu.os-info.name != 'linux' {
		error make {msg: 'building the container image with Tangram requires Linux'}
	}
	let host = $platforms | get --optional $platform
	if $host == null {
		error make {msg: $'unsupported platform ($platform), expected one of ($platforms | columns | str join ", ")'}
	}

	# Build the image, which reports the commit of this checkout. Name the host only for a cross build, because std spells a native host like the machine.
	let commit = ^python3 scripts/version.py | str trim
	let arg = {env: {TANGRAM_CLI_COMMIT_HASH: $commit}}
	let arg = if $platform == (host-platform) { $arg } else { $arg | insert host $host }
	let arg = if $fast { $arg | merge {profile: fast, proxy: true} } else { $arg }
	let temporary = ^mktemp -d | str trim
	let image = $temporary | path join image.tar
	try {
		^tg build ./build.tg.ts#image --arg-value ($arg | to json --raw) $'--checkout=($image)'

		# Load the image, which has no name of its own, and tag it.
		let loaded = ^docker load --input $image | lines | last
		let id = $loaded | parse --regex 'sha256:(?<id>[0-9a-f]+)' | get id | first
		^docker tag $id $tag
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
	print $'loaded ($tag) for ($platform)'
}

# Get the Docker platform of this machine.
def host-platform [] {
	match (^uname -m | str trim) {
		'x86_64' => 'linux/amd64'
		'aarch64' | 'arm64' => 'linux/arm64'
		$machine => { error make {msg: $'unsupported machine ($machine)'} }
	}
}
