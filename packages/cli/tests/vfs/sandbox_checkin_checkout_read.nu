use ../../test.nu *

# A sandboxed process can read back an artifact it checked in itself, through the path returned by
# an internal checkout. The standard library's linker proxy depends on this: it checks in the file
# it has just linked, checks it out to obtain a path, and then copies that path in order to embed
# the wrapper.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server = server spawn --busybox --config { vfs: true }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			return tg.build`
				path="\${TMPDIR:-/tmp}/hello.txt"
				echo "Hello, World!" > $path
				id=$(tg checkin $path)
				checkout=$(tg checkout $id)
				cat "$checkout" > ${tg.output}
			`.env(tg.build(busybox));
		}
	',
}

let output = tg build $path | complete
success $output 'the process should read back the artifact it checked in'

let contents = tg read ($output.stdout | str trim)
snapshot $contents 'Hello, World!'
