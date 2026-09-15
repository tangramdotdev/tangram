use ../../test.nu *

# An environment with directory handles shared across path mutations fits in a child process argument.

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
			return tg.build(child, {
				env: {
					LIBRARY_PATH: path("lib"),
					PATH: path("bin"),
				},
			});
		}
	'
}

let output = tg build $path | complete
success $output
snapshot $output.stdout '
	true

'
