use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(100);
			}
		};
	'
}

let process = tg build -dv $path | from json
tg cancel $process.process $process.token
let output = tg wait $process.process | complete
let output = $output.stdout | str trim | from json
snapshot $output.error.message 'the process was canceled'

let id = job spawn {
	tg build $path | complete
};
job kill $id

let output = tg wait $process.process | complete
let output = $output.stdout | str trim | from json
snapshot $output.error.message 'the process was canceled'

let path = artifact {
	tangram.ts: '
		export default async () => {
			await Promise.race([
				tg.sleep(0),
				tg.build(child), 
			]);
		};

		export let child = async () => {
			await tg.sleep(100);
		}
	'
}
let id = tg build -d $path
tg wait $id
let children = tg children $id | from json
assert equal $children []
