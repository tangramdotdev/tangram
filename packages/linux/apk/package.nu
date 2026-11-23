#!/usr/bin/env nu

def main [tarball: path, architecture: string, version: string] {
	let source = $env.FILE_PWD
	let version = $version | str replace --regex '-(alpha|beta|pre|rc)\.?([0-9]*)$' '_${1}${2}'
	if $architecture not-in [aarch64 x86_64] or $version !~ '^[0-9][0-9A-Za-z._]*$' {
		error make {msg: 'invalid APK architecture or version'}
	}
	let version = $'($version)-r0'
	let destination = ('release/apk' | path expand | path join $architecture)
	mkdir $destination
	let temporary = ^mktemp -d ($destination | path join '.apk.XXXXXXXX') | str trim
	try {
		^ln $tarball ($temporary | path join tangram.tar.gz)
		^cp ($source | path join ../config.json) ($temporary | path join config.json)
		^cp ($source | path join ../openrc/tangram) ($temporary | path join tangram)
		^cp ($source | path join ../../../LICENSE) ($temporary | path join LICENSE)
		^cp ($source | path join pack.sh) ($temporary | path join pack.sh)
		for script in [pre-install post-install post-upgrade pre-deinstall] {
			^install -m 755 ($source | path join $script) ($temporary | path join $'.($script)')
		}
		if (which abuild-tar | is-empty) or (which apk | is-empty) {
			let image = ('
FROM alpine:3.22
RUN apk add --no-cache abuild tar
' | ^docker build --quiet - | str trim)
			let uid = ^id -u | str trim
			let gid = ^id -g | str trim
			^docker run --rm --user $'($uid):($gid)' --volume $'($temporary):/build' --workdir /build $image sh pack.sh $architecture $version
		} else {
			cd $temporary
			^sh pack.sh $architecture $version
		}
		let name = $'tangramdotdev-($version).apk'
		let output = $destination | path join $name
		if ($output | path type) == dir {
			error make {msg: $'the release destination is a directory ($output)'}
		}
		mv -f ($temporary | path join $name) $output
		print $'packaged ($output)'
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
}
