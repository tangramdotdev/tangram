use ../../test.nu *

# A sandbox handle can create a sandbox and run a command in it.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await using sandbox = await tg.Sandbox.create();
			await sandbox.run`echo hello`;
			await sandbox.run`echo world`;
			console.log(sandbox.id);
			{
				await using destroyed = await tg.Sandbox.create();
				await tg.client.destroySandbox(destroyed.id);
			}
		}
	',
}

let output = tg exec $path | complete
success $output
let lines = $output.stdout | lines
assert (($lines | get 0) == "hello")
assert (($lines | get 1) == "world")
let sandbox = $lines | get 2
assert ($sandbox | str starts-with "sbx_")
let state = tg sandbox get $sandbox | from json
assert equal $state.status "destroyed" "the sandbox should be destroyed when its handle is disposed"
