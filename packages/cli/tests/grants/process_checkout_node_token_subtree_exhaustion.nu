use ../../test.nu *

# A process that stores an output whose dependency was reconstructed from an id receives only a node token for that output, so checking the output out must prove its subtree. That proof must not fail because unrelated artifacts in the store share the dependency's directory.

let server = server spawn --config {
	index: { map_size: 536_870_912 }
	store: { map_size: 536_870_912 }
}

let path = artifact {
	tangram.ts: '
		export default async (mark: string) => {
			const loader = await tg.file("loader");
			const hub = await tg.directory({ loader });
			// Build a toolchain whose closure is wider than the descendant search budget.
			const entries: Record<string, tg.Unresolved<tg.Directory.Arg>> = {
				lib: { inner: hub },
			};
			for (let index = 0; index < 4000; index++) {
				entries[`f${index}`] = { [`f${index}`]: tg.file(`entry ${index}`) };
			}
			const sdk = await tg.directory(entries);
			const contents = await tg.blob(`output ${mark}`);
			// Reconstruct the dependency from its id, as a wrapper does from a rendered path, so its referent carries no token and the store grants only the node.
			const dependency = tg.File.withId(loader.id);
			const output = await tg.file({
				contents,
				dependencies: { [loader.id]: dependency },
			});
			await output.store();
			const reference = tg.Referent.toDataString(
				tg.Object.toReferent(output),
				id => id,
			);
			return tg.command({
				args: ["checkout", "--dependencies=false", "--path", tg.output, reference],
				env: { CONTENTS: contents, SDK: sdk },
				executable: "tg",
				host: tg.host.current,
			});
		};
	'
}

let command = tg build $path -a clean | str trim
let output = tg build $command | complete
success $output 'the checkout must succeed against a clean store'

# Accumulate unrelated artifacts that contain the same dependency directory.
for chunk in 0..<12 {
	let entries = 0..<500 | each { |i|
		let n = [($chunk | into string) '_' ($i | into string)] | str join
		['"d' $n '": tg.directory({"f' $n '": tg.directory({"loader": tg.file("loader")})})'] | str join
	} | str join ','
	tg put (['tg.directory({' $entries '})'] | str join) | ignore
}
tg index

let command = tg build $path -a ambient | str trim
let output = tg build $command | complete
success $output 'a checkout must not fail because unrelated artifacts exhausted the subtree authorization search'
