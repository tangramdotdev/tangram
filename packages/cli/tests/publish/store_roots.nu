use ../../test.nu *

# Equal subpaths in different artifact roots must not collide in the publishing graph.

let server = server spawn
let module = artifact {
	tangram.ts: '
		export default async () => {
			const leaf = await tg.directory({
				"tangram.ts": tg.file({
					contents: "export const metadata = { tag: \"test-leaf/1.0.0\" };",
					module: "ts",
				}),
			});
			const dependency = await tg.directory({
				"tangram.ts": tg.file({
					contents: "export const metadata = { tag: \"test-dep/1.0.0\" };",
					dependencies: {
						"test-leaf?source=../leaf": { node: leaf, options: { path: "../leaf" } },
					},
					module: "ts",
				}),
			});
			const root = await tg.directory({ leaf, pkg: dependency });
			const main = await tg.directory({
				"tangram.ts": tg.file({
					contents: "export const metadata = { tag: \"test-main/1.0.0\" };",
					dependencies: {
						"test-dep?source=../pkg": {
							node: dependency,
							options: { id: root.id, path: "pkg" },
						},
					},
					module: "ts",
				}),
			});
			return tg.directory({ dependency: root, pkg: main });
		};
	'
}
let id = tg build $module | str trim
let root = tg checkout $id | str trim
let elsewhere = mktemp --directory
cd $elsewhere
let plan = tg publish --dry-run ($root | path join pkg) | from json
assert equal ($plan | get tag) ['test-leaf/1.0.0' 'test-dep/1.0.0' 'test-main/1.0.0']
let dependency = $plan.1
let main = $plan.2
assert ($dependency.referent.options.id != $main.referent.options.id)
assert equal $dependency.referent.options.path pkg
assert equal $main.referent.options.path pkg
let dependency_root = tg checkout $dependency.referent.options.id | str trim
assert equal $dependency.path ($dependency_root | path join pkg)
assert equal $main.path ($root | path join pkg)
