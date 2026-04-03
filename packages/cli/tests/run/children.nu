use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export const child = () => "child";
		export default async () => {
			return await tg.build(child).named("child");
		};
	'#
}

let process = tg build -dv $path | from json
tg wait $process.process

let output = tg children $process.process | complete
success $output
let children = $output.stdout | from json
let child = $children | get 0 | update process 'PROCESS'
snapshot -n children $child '
	options: name: child
	process: PROCESS

'
