use ../lib/test.nu *

# Checkin seeds the sandbox VFS with its issued token before returning the artifact ID.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let module = artifact {
	tangram.ts: '
		export default () => tg.run`
			set -e
			file="$TMPDIR/checkin.txt"
			printf "checked in\n" > "$file"
			id=$(tg checkin "$file")
			IFS= read -r value < "/opt/tangram/store/$id"
			printf "%s\n" "$value"
		`.sandbox();
	'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
let configs = [false] | append ($transports | each { |io| { io: $io, kind: fuse, passthrough: disabled } })
for vfs in $configs {
	let local = server spawn --config {
		# Disable fallback searches so a missing checkin token fails deterministically.
		verification: { permissions: { final: false, initial: false } }
		vfs: $vfs
	}
	let output = tg run $module | complete
	server stop $local
	success $output $'the process must read the file it checked in with vfs = ($vfs | to nuon)'
	assert equal ($output.stdout | str trim) 'checked in'
}
