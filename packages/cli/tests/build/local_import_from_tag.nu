use ../../test.nu *

# Test that imports with local attributes resolve correctly when building from a published tag.
# Building from a local path works, but building from a tag fails to resolve the sibling dependency.

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

let path = artifact {
	"main.tg.ts": '
		import dep from "dep" with { local: "./dep.tg.ts" };
		export default () => dep();
		export const metadata = { tag: "main/1.0.0" };
	'
	"dep.tg.ts": '
		export default () => tg.file("hello");
		export const metadata = { tag: "dep/1.0.0" };
	'
}

# Publish both packages.
tg publish ($path | path join 'dep.tg.ts')
tg publish ($path | path join 'main.tg.ts')

# Build from local path works.
let local_output = tg build ($path | path join './main.tg.ts') | complete
success $local_output "building from local path should succeed"

# Build from tag fails.
let tag_output = tg build main/1.0.0 | complete
success $tag_output "building from tag should succeed"
