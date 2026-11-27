#!/bin/sh
set -eu
set -o pipefail
umask 022

architecture=$1
version=$2
mtime=@${SOURCE_DATE_EPOCH:?set SOURCE_DATE_EPOCH to the time to give every file}
apk version --check "$version"

cut_tar() {
	abuild-tar --cut "$@"
	# Consume the remaining tar records so the producer can finish writing.
	cat >/dev/null
}

mkdir -p root/usr/bin root/etc/tangram root/etc/init.d root/etc/logrotate.d root/usr/share/licenses/tangram
tar -xzf tangram.tar.gz -C root/usr/bin tangram tg tgx
install -m 644 config.json root/etc/tangram/config.json
install -m 755 tangram root/etc/init.d/tangram
install -m 644 logrotate root/etc/logrotate.d/tangram
install -m 644 LICENSE root/usr/share/licenses/tangram/LICENSE

# Use Alpine's tar postprocessor to write the APK payload checksums.
(
	cd root
	find etc usr -print0 |
		LC_ALL=C sort -z |
		tar --format=pax --pax-option=exthdr.name=%d/PaxHeaders/%f,atime:=0,ctime:=0 \
			--mtime="$mtime" --owner=0 --group=0 --numeric-owner --blocking-factor=1 --null --no-recursion -cf - -T -
) | {
	cut_tar --hash
	head -c 1024 /dev/zero
} | gzip -n > data.tar.gz

# Write the package information, with the installed size counted in kibibytes from the sizes of the files, so that it does not depend on the filesystem.
cat > .PKGINFO <<PKGINFO
pkgname = tangramdotdev
pkgver = $version
pkgdesc = Tangram build system and package manager
url = https://tangram.dev
size = $(find root ! -type d -printf '%s\n' | awk '{ size += int(($1 + 1023) / 1024) } END { print size * 1024 }')
arch = $architecture
origin = tangramdotdev
maintainer = Tangram <support@tangram.dev>
license = MIT
depend = ca-certificates
depend = fuse3
depend = iptables
depend = libgcc
depend = musl
depend = nftables
depend = openrc
depend = passt
depend = squashfs-tools
datahash = $(sha256sum data.tar.gz | cut -d ' ' -f 1)
PKGINFO

# Leave signing to the release script. APK v2 joins the control and payload gzip streams.
tar --format=ustar --mtime="$mtime" --owner=0 --group=0 --numeric-owner --blocking-factor=1 -cf - \
	.PKGINFO .pre-install .post-install .post-upgrade .pre-deinstall |
	cut_tar | gzip -n > control.tar.gz
cat control.tar.gz data.tar.gz > "tangramdotdev-$version.apk"
