use ../../test.nu *

# A build can check in a file created inside the sandbox and the resulting artifact contains the expected contents.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.build`
				path="\${TMPDIR:-/tmp}/hello.txt"
				echo "Hello, World!" > $path
				tg checkin $path > ${tg.output}
			`;
		}
	',
}

let output = tg build $path
let id = tg read $output
let contents = tg read $id
snapshot $contents 'Hello, World!'
