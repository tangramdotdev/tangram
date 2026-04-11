use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let promise = new Promise(() => reject(new Error("kaboom")));
			promise.catch(() => {});
			await new Promise(() => {});
		};
	'
}

cd $path
let output = timeout 10s tg build | complete
failure $output
assert ($output.stderr | str contains "kaboom")
