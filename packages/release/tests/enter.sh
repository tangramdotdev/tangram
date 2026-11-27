#!/bin/sh
set -eu

# Run a command as the first process of a new PID namespace whose root is a root filesystem. Move the root filesystem over the namespace's root before changing the root to it, as switch_root does, because the kernel refuses to create a user namespace under a root that another mount covers or that is not the top of the namespace's root, and the server creates one for each sandbox.
root=$1
shift
mkdir -p "$root/dev" "$root/proc" "$root/run" "$root/sys" "$root/tmp"

# Keep the sandbox's user namespace when it maps subordinate IDs, so that the root filesystem has every ID that the sandbox maps, and otherwise map root to the sandbox's one ID in a new user namespace.
ids() {
	count=0
	while read -r _ _ length; do
		count=$((count + length))
	done < "$1"
	echo "$count"
}
user=--map-root-user
if [ "$(ids /proc/self/uid_map)" -gt 1 ] && [ "$(ids /proc/self/gid_map)" -gt 1 ]; then
	if [ "$(id -u)" -ne 0 ]; then
		echo "expected the sandbox to run as root when it maps subordinate IDs" >&2
		exit 1
	fi
	user=
fi
exec unshare $user --mount --pid --fork --kill-child sh -euc '
	root=$1
	shift
	mount --rbind "$root" "$root"
	mount -t proc proc "$root/proc"
	mount --rbind /dev "$root/dev"
	mount --rbind /sys "$root/sys"
	mount -t tmpfs tmpfs "$root/run"
	cd "$root"
	mount --move . /
	exec chroot . "$@"
' sh "$root" "$@"
