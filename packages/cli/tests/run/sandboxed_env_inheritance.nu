use ../../test.nu *

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
