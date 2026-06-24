use ../../test.nu *

# A build running one hundred commands concurrently completes without hanging or
# failing.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let results = await Promise.all(Array.from(Array(100).keys()).map((i) => tg.run(double, i)));
			return results.reduce((acc, el) => acc + el, 0);
		}
		export function double(i: number) { return i * 2; }
	'
}

let output = tg build $path | complete
success $output
