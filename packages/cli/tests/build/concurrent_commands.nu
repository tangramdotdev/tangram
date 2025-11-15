use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let results = await Promise.all(Array.from(Array(100).keys()).map((i) => tg.run(double, i)));
			return results.reduce((acc, el) => acc + el, 0);
		};
		export let double = (i: number) => i * 2;
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
