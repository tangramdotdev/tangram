use ../../test.nu *

# A sandboxed process does not inherit environment variables from the client's environment.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`echo $\{FOO:-unset\}`.sandbox();
		}
	',
}

let output = (with-env { FOO: inherited } { tg run $path | complete })
success $output
snapshot $output.stdout '
	unset

'
