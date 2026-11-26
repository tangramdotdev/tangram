#!/usr/bin/env nu

def main [tarball: path, architecture: string, version: string] {
	let source = $env.FILE_PWD
	let destination = ('release/apt' | path expand)
	mkdir $destination
	let temporary = ^mktemp -d ($destination | path join '.deb.XXXXXXXX') | str trim
	try {
		let root = $temporary | path join package
		let control = $root | path join DEBIAN
		let bin = $root | path join usr bin
		let config = $root | path join etc tangram
		let units = $root | path join usr lib systemd system
		mkdir $control $bin $config $units
		^tar -xzf $tarball -C $bin tangram tg tgx
		open ($source | path join ../config.json) | insert manager systemd | to json --tabs 1 | save ($config | path join config.json)
		^chmod 644 ($config | path join config.json)
		^install -m 644 ($source | path join ../systemd/tangram.service) ($units | path join tangram.service)
		for script in [postinst prerm postrm] {
			^install -m 755 ($source | path join $script) ($control | path join $script)
		}
		'/etc/tangram/config.json
' | save ($control | path join conffiles)
		$'Package: tangram
Version: ($version)
Architecture: ($architecture)
Maintainer: Tangram <support@tangram.dev>
Depends: adduser, ca-certificates, fuse3, init-system-helpers, iptables, libcap-ng0, libc6, libgcc-s1, nftables, passt, squashfs-tools, systemd
Homepage: https://tangram.dev
Description: Tangram build system and package manager
 Includes the Tangram CLI and a systemd-managed server.
' | save ($control | path join control)
		let name = $'tangram_($version)_($architecture).deb'
		let archive = $temporary | path join $name
		if (which dpkg-deb | is-empty) {
			^docker run --rm --volume $'($temporary):/build' debian:trixie-slim dpkg-deb --root-owner-group -Zgzip --build /build/package $'/build/($name)'
		} else {
			^dpkg-deb --root-owner-group -Zgzip --build $root $archive
		}
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
