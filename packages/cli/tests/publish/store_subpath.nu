use ../lib/test.nu *

# Publishing a store subpath preserves its root through nested files and source dependencies.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let source = artifact {
	packages: {
		dep: {
			tangram.ts: '
				import leaf from "test-leaf" with { source: "../leaf" };
				export default () => leaf();
				export const metadata = { tag: "test-dep/1.0.0" };
			'
		}
		leaf: {
			tangram.ts: '
				export default () => "dependency";
				export const metadata = { tag: "test-leaf/1.0.0" };
			'
		}
		main: {
			lib: {
				entry.tg.ts: '
					import dep from "test-dep" with { source: "../../dep" };
					export default () => dep();
				'
			}
			tangram.ts: '
				import entry from "./lib/entry.tg.ts";
				export default () => entry();
				export const metadata = { tag: "test-main/1.0.0" };
			'
		}
	}
}
let id = tg checkin $source | str trim
let root = tg checkout $id | str trim
let main = $root | path join packages main
let main_id = tg checkin $main | str trim
let dep_id = tg checkin ($root | path join packages dep) | str trim
let leaf_id = tg checkin ($root | path join packages leaf) | str trim

# An unrelated working directory must not change the planned or published artifacts.
let elsewhere = artifact {
	packages: {
		main: { tangram.ts: 'export default () => "wrong main";' }
		dep: { tangram.ts: 'export default () => "wrong dependency";' }
	}
}
cd $elsewhere
let plan = tg publish --dry-run $main | from json
for name in [main dep] {
	let item = $plan | where tag == $'test-($name)/1.0.0' | first
	assert equal $item.path ($root | path join packages $name)
	assert equal $item.referent.options.id $id
	assert equal $item.referent.options.path $'packages/($name)'
}
tg publish $main
for package in [
	{ tag: 'test-main/1.0.0', id: $main_id }
	{ tag: 'test-dep/1.0.0', id: $dep_id }
	{ tag: 'test-leaf/1.0.0', id: $leaf_id }
] {
	assert equal (tg tag get $package.tag | from json | get target.id) $package.id
	assert equal (tg --url $remote.url tag get $package.tag | from json | get target.id) $package.id
}
