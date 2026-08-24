use ../../test.nu *

# A readonly sandbox bind recursively protects nested mounts.

if $nu.os-info.name != 'linux' {
	return
}

let source = mktemp --directory | str trim
let target = mktemp --directory | str trim
let script = r#'
	set -eu
	mount --make-rprivate /
	mount -t tmpfs -o mode=0755 tmpfs "$1"
	mkdir "$1/nested"
	mount -t tmpfs -o mode=0777 tmpfs "$1/nested"
	printf original > "$1/nested/file"
	exec tangram sandbox container run \
		--index 0 \
		--unshare-all \
		--uid 0 \
		--gid 0 \
		--chdir / \
		--ro-bind "$1" "$2" \
		-- /bin/sh -c '
			if touch "$1/top" 2>/dev/null; then
				exit 10
			fi
			if printf changed > "$1/nested/file" 2>/dev/null; then
				exit 11
			fi
			test "$(cat "$1/nested/file")" = original
		' _ "$2"
'#
let output = ^unshare --user --map-root-user --mount bash -c $script _ $source $target | complete
success $output
