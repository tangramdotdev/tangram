use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
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
