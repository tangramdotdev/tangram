use ../../util/test.nu *

# Publishing a package inside a repository whose root directory is also a package publishes the package and its dependency, even when another package in the repository imports a package that has no tag.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create a repository with a root module and packages, where an unrelated package imports a package that is not published.
let root = artifact {
	tangram.ts: '
		export default function () { return "scripts"; }
	'
	packages: {
		dep: {
			tangram.ts: '
				export let metadata = { tag: "dep/1.0.0" };
			'
		}
		main: {
			tangram.ts: '
				import dep from "dep" with { source: "../dep" };
				export let metadata = { tag: "main/1.0.0" };
			'
		}
		other: {
			tangram.ts: '
				import unpublished from "unpublished" with { source: "../unpublished" };
				export let metadata = { tag: "other/1.0.0" };
			'
		}
		unpublished: {
			tangram.ts: '
				export let metadata = { tag: "unpublished/1.0.0" };
			'
		}
	}
}

# Publish the package.
let output = tg publish ($root | path join packages/main) | complete
success $output "the publish should succeed"

# The package and its dependency are tagged on the remote with the local targets.
for tag in [dep/1.0.0 main/1.0.0] {
	let local_target = tg tag get $tag | from json | get target.id
	let remote_target = tg --url $remote.url tag get $tag | from json | get target.id
	assert equal $remote_target $local_target $"the remote tag ($tag) should target the local package"
}

# The other packages in the repository are not published.
for tag in [other/1.0.0 unpublished/1.0.0] {
	let output = tg --url $remote.url tag get $tag | complete
	failure $output $"the tag ($tag) should not be published"
}
