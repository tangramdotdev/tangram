use ../../test.nu *

# The results of commands run concurrently with `Promise.all` are
# all returned and can be aggregated correctly.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let results = await Promise.all(Array.from(Array(5).keys()).map((i) => tg.run(double, i)));
			return results.reduce((acc, el) => acc + el, 0);
		};
		export let double = (i: number) => i * 2;
	'
}

let output = tg build $path
snapshot $output
