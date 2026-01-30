use ../../test.nu *

# Reproduces a bug where reading a compacted log fails with early eof.

let remote = spawn -n remote

let path = artifact {
	tangram.ts: '
		export default () => {
			for (let i = 0; i < 9900; i++) {
				console.log(`Line ${i.toString().padStart(4, "0")}: ${"x".repeat(200)}`);
			}
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg remote put default $remote.url | complete
tg push --logs $id

# Read from remote blob should not fail with early eof.
let output = tg --url $remote.url process log $id | complete
success $output "Log read failed"
