use ../../lib/test.nu *

# Reproduce the JS pause while storing a new directory containing previously stored files.
# Count comparisons before the batch reaches the server, without changing token decisions.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const files = await Promise.all(
				Array.from({ length: 32 }, (_, i) => tg.file(`file ${i}`)),
			);
			await tg.Value.store(files);

			async function measure(count: number, storeDirectory: boolean) {
				const directory = await tg.directory(Object.fromEntries(
					files.slice(0, count).map((file, i) => [`file${i}`, file]),
				));
				if (storeDirectory) await directory.store();
				const output = await tg.directory({ directory });

				const covers = tg.Authorization.Token.covers;
				const postObjectBatch = tg.client.postObjectBatch;
				let comparisons = 0;
				let comparisonsBeforeRequest = 0;
				let requests = 0;
				try {
					tg.Authorization.Token.covers = (...args) => {
						comparisons++;
						return covers(...args);
					};
					tg.client.postObjectBatch = async (arg) => {
						comparisonsBeforeRequest = comparisons;
						requests++;
						return postObjectBatch.call(tg.client, arg);
					};
					await output.store();
				} finally {
					tg.Authorization.Token.covers = covers;
					tg.client.postObjectBatch = postObjectBatch;
				}

				// Confirm that the normal store request completed and its output is readable.
				tg.assert(requests === 1);
				output.unload();
				const file = await output.get(`directory/file${count - 1}`);
				tg.File.assert(file);
				tg.assert(await file.text === `file ${count - 1}`);
				return { comparisons: comparisonsBeforeRequest, id: output.id };
			}

			const small = await measure(16, false);
			const large = await measure(32, false);
			const control = await measure(32, true);
			tg.assert(large.id === control.id);
			return {
				small: small.comparisons,
				large: large.comparisons,
				control: control.comparisons,
			};
		}
	'
}

let output = tg build $path | complete
success $output
let counts = $output.stdout | from json
print $counts
assert ($counts.small > 0)
assert ($counts.control < $counts.small) 'storing the inner directory first should provide a covering proof'
assert ($counts.large <= 4 * $counts.small) 'doubling the files should not require roughly eight times as many token comparisons before sending the store request'
