use ../../util/test.nu *

# A sandbox handle can stop and start its sandbox, and the sandbox runs commands after it starts again.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await using sandbox = await tg.Sandbox.create();
			await sandbox.stop();
			await sandbox.stop();
			await sandbox.start();
			await sandbox.start();
			await sandbox.run`echo hello`;
		}
	',
}

let output = tg exec $path | complete
success $output
assert (($output.stdout | lines | get 0) == "hello")
