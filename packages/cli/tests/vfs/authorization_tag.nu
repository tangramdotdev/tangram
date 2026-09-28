use ../lib/test.nu *

# The VFS can authorize a tag target using the tag token returned by enumeration.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --config {
		advanced: { checkpoints: true }
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}
	let target = tg put 'tg.file("allowed\n")' | str trim
	let foreign = tg put 'tg.file("foreign\n")' | str trim
	tg tag shared $target
	let sandbox = tg sandbox create --no-network | str trim
	tg grant $sandbox read shared | ignore
	tg index

	let module = artifact {
		tangram.ts: '
			export default function (target: string, foreign: string) {
				return tg.command({
					args: ["-ec", `
						for entry in /opt/tangram/store/*; do :; done
						IFS= read -r value < "$1"
						test "$value" = allowed
						if (IFS= read -r value < "$2") 2>/dev/null; then exit 1; fi
						printf "ok\n"
					`, "sh", `/opt/tangram/store/${target}`, `/opt/tangram/store/${foreign}`],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}
	let command = tg build $module --arg-string $target --arg-string $foreign | str trim
	let watch = tg checkpoint watch tag.get.read | from json | get watch
	let output = timeout 15s tg run $'--sandbox=($sandbox)' $command | complete
	success $output 'the tag target must be readable through the VFS authorization fallback'
	assert equal ($output.stdout | str trim) 'ok'
	tg checkpoint unwatch tag.get.read $watch
	server stop $local
}
