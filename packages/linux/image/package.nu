#!/usr/bin/env nu

const repository = 'ghcr.io/tangramdotdev/tangram'

def main [tarball: path, architecture: string, version: string] {
	let source = $env.FILE_PWD
	let destination = ('release/image' | path expand)
	if $architecture not-in [amd64 arm64] {
		error make {msg: 'invalid image architecture'}
	}
	let tag = $'($repository):($version | str replace --all --regex "[^0-9A-Za-z_.-]" "-")-($architecture)'
	mkdir $destination
	let temporary = ^mktemp -d ($destination | path join '.image.XXXXXXXX') | str trim
	try {
		let context = $temporary | path join context
		mkdir $context
		^tar -xzf $tarball -C $context tangram tgx
		^install -m 644 ($source | path join ../config.json) ($context | path join config.json)
		^docker buildx build --build-arg $'TANGRAM_VERSION=($version)' --file ($source | path join Dockerfile) --load --platform $'linux/($architecture)' --tag $tag $context
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
