use ../../test.nu *

# An environment composed from child build outputs preserves its artifact references through all three process output readers.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			let env;
			for (const sandbox of [true, false]) {
				env = await tg.run(environment).sandbox(sandbox);
				for (const [name, suffix] of [["PATH", "bin"], ["CPATH", "include"], ["LIBRARY_PATH", "lib"]]) {
					const mutation = env[name];
					tg.assert(mutation instanceof tg.Mutation);
					tg.assert(mutation.inner.kind === "prefix");
					tg.assert(mutation.inner.separator === ":");
					const directories = mutation.inner.template.components.filter((component) => component instanceof tg.Directory);
					tg.assert(directories.length === 40);
					for (const [index, directory] of directories.entries()) {
						const file = tg.File.expect(await directory.get(`${suffix}/entry`));
						tg.assert(await file.text === `${index}:${suffix}`);
					}
				}
			}
			return env;
		}

		export async function environment() {
			// Compose the child utility artifacts as std.utils.env does.
			const directories = await Promise.all(Array.from({ length: 40 }, (_, index) => tg.build(utility, index)));
			const env = {};
			for (const [name, suffix] of [["PATH", "bin"], ["CPATH", "include"], ["LIBRARY_PATH", "lib"]]) {
				env[name] = await tg.Mutation.prefix(tg.Template.join(":", ...directories.map((directory) => tg`${directory}/${suffix}`)), ":");
			}
			// Exceed the 64 KiB limit even on tmpfs.
			tg.assert(tg.encoding.utf8.encode(tg.Value.print(env)).length > 65_536);
			return env;
		}

		export async function utility(index: number) {
			return tg.directory({
				"bin/entry": tg.file(`${index}:bin`),
				"include/entry": tg.file(`${index}:include`),
				"lib/entry": tg.file(`${index}:lib`),
			});
		}
	'
}

let output = tg run --no-sandbox --no-tokens $path | complete
success $output
assert ($output.stdout | str contains 'mutation(') 'expected the returned environment'
