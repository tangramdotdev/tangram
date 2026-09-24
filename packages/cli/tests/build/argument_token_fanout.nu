use ../lib/test.nu *

# Argument authorization tokens stay on their objects instead of spreading to siblings through the command.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export async function child(arg: tg.Value) {
			for (const object of tg.Value.objects(arg)) {
				tg.Directory.assert(object);
				await object.entries;
			}
			return true;
		}
		export default async function () {
			const directories = await Promise.all(
				Array.from({ length: 13 }, async (_, i) => {
					const directory = await tg.directory({ file: `${i}` });
					await directory.store();
					return directory;
				}),
			);
			const path = (suffix: string) =>
				tg.Mutation.prefix(
					tg.Template.join(
						":",
						...directories.map((directory) => tg`${directory}/${suffix}`),
					),
					":",
				);
			const command = await tg.command(child, {
				env: {
					LIBRARY_PATH: path("lib"),
					PATH: path("bin"),
				},
			});
			const tokens = new Set(
				directories.flatMap((directory) =>
					Object.values(directory.state.tokens).flatMap((entry) => entry),
				),
			);
			tg.assert(tokens.size >= directories.length);
			for (const entry of Object.values(command.state.tokens)) {
				for (const token of entry) {
					tg.assert(!tokens.has(token), "the command inherited an argument authorization token");
				}
			}
			return tg.build(command);
		}
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout '
	true

'
