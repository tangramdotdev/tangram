#!/usr/bin/env nu

def main [tarball: path, architecture: string, version: string] {
	let source = $env.FILE_PWD
	let destination = ('release/rpm' | path expand)
	let version = $version | str replace --all '-' '~'
	if $architecture not-in [aarch64 x86_64] or $version !~ '^[0-9][0-9A-Za-z.+~]*$' {
		error make {msg: 'invalid RPM architecture or version'}
	}
	mkdir $destination
	let temporary = ^mktemp -d ($destination | path join '.rpm.XXXXXXXX') | str trim
	try {
		let sources = $temporary | path join SOURCES
		let spec = $temporary | path join tangram.spec
		mkdir $sources
		^cp $tarball ($sources | path join tangram.tar.gz)
		^cp ($source | path join ../config.json) ($sources | path join config.json)
		^cp ($source | path join ../systemd/tangram.service) ($sources | path join tangram.service)
		^cp ($source | path join ../../../LICENSE) ($sources | path join LICENSE)
		^cp ($source | path join tangram.spec) $spec
		let args = [--define $"tangram_version ($version)" --target $architecture -bb]
		if (which rpmbuild | is-empty) {
			let image = ('
FROM fedora:45
RUN dnf -qy --setopt=install_weak_deps=False install rpm-build && dnf clean all
' | ^docker build --quiet - | str trim)
			let uid = ^id -u | str trim
			let gid = ^id -g | str trim
			^docker run --rm --user $"($uid):($gid)" --volume $"($temporary):/build" $image rpmbuild --define '_topdir /build' ...$args /build/tangram.spec
		} else {
			^rpmbuild --define $"_topdir ($temporary)" ...$args $spec
		}
		let name = $'tangram-($version)-1.($architecture).rpm'
		let archive = $temporary | path join RPMS $architecture $name
		let output = $destination | path join $name
		if ($output | path type) == dir {
			error make {msg: $'the release destination is a directory ($output)'}
		}
		mv -f $archive $output
		print $'packaged ($output)'
	} catch {|error|
		rm -r $temporary
		error make $error.raw
	}
	rm -r $temporary
}
