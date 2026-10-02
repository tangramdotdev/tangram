use ../lib/test.nu *

# A branch directory renews the tokens of a leaf that an earlier lookup reused.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --now '2026-10-02T12:00:00Z' --config {
		object: { permission_time_to_live: 5 }
		verification: {
			permissions: {
				final: false
				initial: false
			}
		}
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let module = artifact {
		tangram.ts: '
			export default async function (name: string) {
				const directory = await tg.directory({
					children: [
						{ count: 2, directory: await tg.directory({ a: "a\n", b: "b\n" }), last: "b" },
						{ count: 2, directory: await tg.directory({ c: "c\n", d: "d\n" }), last: "d" },
					],
				});
				return tg.command({
					args: ["-ec", `
						IFS= read -r value < "$1/$2"
						test "$value" = "$2"
						printf "ok\n"
					`, "sh", directory, name],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}

	let first = tg build $module --arg-string a | str trim
	let second = tg build $module --arg-string b | str trim
	let sandbox = tg sandbox create --no-network | str trim
	let output = tg run $'--sandbox=($sandbox)' $first | complete
	success $output 'the first process must read an entry of the leaf'
	assert equal ($output.stdout | str trim) 'ok'

	advance_time $local 6sec
	let output = tg run $'--sandbox=($sandbox)' $second | complete
	success $output 'the reused leaf must authorize another entry after token renewal'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
