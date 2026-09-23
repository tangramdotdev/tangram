use ../../lib/test.nu *

# Reproduce superquadratic token comparison growth while storing a directory.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			async function measure(count: number) {
				const files = await Promise.all(Array.from({ length: count }, (_, i) => tg.file(`${i}`)));
				await tg.Value.store(files);
				const output = await tg.directory({ dir: Object.fromEntries(files.map((file, i) => [String(i), file])) });
				const covers = tg.Authorization.Token.covers;
				let comparisons = 0;
				tg.Authorization.Token.covers = (...args) => { comparisons++; return covers(...args); };
				try { await output.store(); }
				finally { tg.Authorization.Token.covers = covers; }
				return comparisons;
			}
			return [await measure(4), await measure(8)];
		}
	'
}

let output = tg build $path | complete
success $output
let counts = $output.stdout | from json
print $counts
assert ($counts.1 <= 4 * $counts.0) 'doubling the files should not increase token comparisons more than fourfold'
