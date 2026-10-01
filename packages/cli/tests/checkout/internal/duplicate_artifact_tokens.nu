use ../../lib/test.nu *

# Duplicate references share their tokens without changing the output paths or authorizing other artifacts.

let server = server spawn --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
	vfs: false
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const directory = await tg.directory({});
			const file = await tg.file("other");
			await tg.Value.store([directory, file]);
			const bare = tg.Directory.withId(directory.id);
			for (const artifacts of [[directory, file, bare], [bare, file, directory]]) {
				const nodes = artifacts.map(tg.Object.toReferent);
				const stream = await tg.client.checkout({ nodes });
				const output = await tg.Progress.lastOutput(stream);
				tg.assert(output?.paths.length === 3);
				tg.assert(output.paths[0].endsWith(directory.id));
				tg.assert(output.paths[1].endsWith(file.id));
				tg.assert(output.paths[0] === output.paths[2]);
			}
			for (const artifacts of [[bare, bare], [directory, tg.File.withId(file.id)]]) {
				const nodes = artifacts.map(tg.Object.toReferent);
				let denied = false;
				try {
					const stream = await tg.client.checkout({ nodes });
					await tg.Progress.lastOutput(stream);
				} catch {
					denied = true;
				}
				tg.assert(denied);
			}
			return true;
		}
	'
}

assert equal (tg build $path | from json) true
