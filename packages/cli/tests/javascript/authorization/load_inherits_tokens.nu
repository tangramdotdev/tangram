use ../../lib/test.nu *

# Loading an object preserves child tokens and inherits parent tokens, including tokens added after the object was loaded.
let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async () => {
			const suffix = "010000000000000000000000000000000000000000000000000000";
			const graphId = `gph_${suffix}` as tg.Graph.Id;
			const getObject = tg.client.getObject;
			try {
				for (const [kind, prefix, constructor] of [
					["directory", "dir", tg.Directory],
					["file", "fil", tg.File],
					["symlink", "sym", tg.Symlink],
				] as const) {
					tg.client.getObject = async () => ({
						data: { kind, value: { graph: graphId, index: 0, kind } },
						children: { [graphId]: { tokens: { remote: ["child"] } } },
						tokens: { remote: ["returned"] },
					});
					const object = constructor.withId(`${prefix}_${suffix}` as never);
					object.state.location = { name: "default" };
					object.state.tokens = { remote: ["parent"] };
					for (const token of ["first", "later"]) {
						object.state.inheritTokens({ remote: [token] });
						await object.load();
						const loaded = object.state.object!;
						const children = tg.Object.Object.children(loaded);
						tg.assert(children.length === 1);
						const child = children[0]!;
						tg.assert(child.state.location !== null, "missing inherited location");
						tg.assert(tg.Location.toDataString(child.state.location) === "remote");
						const tokens = child.state.tokens.remote!;
						for (const expected of ["child", "parent", "returned", "first", token]) {
							tg.assert(tokens.includes(expected));
						}
					}
				}
				const child = tg.File.withId(`fil_${suffix}` as tg.File.Id);
				const file = tg.File.withObject({
					contents: tg.Blob.withId(`blb_${suffix}` as tg.Blob.Id),
					dependencies: {
						"./dependency": { node: child, options: { location: { name: "dependency" } } },
					},
					executable: false,
					module: null,
				});
				file.state.location = { name: "parent" };
				await file.dependencies;
				tg.assert(tg.Location.toDataString(child.state.location!) === "remote:dependency");
				return true;
			} finally {
				tg.client.getObject = getObject;
			}
		};
	',
}
let output = tg --url $local.url build $path | complete
success $output
assert equal ($output.stdout | str trim) 'true'
