use ../../util/test.nu *

# Publishing several paths publishes them as one publish: each package and each shared dependency is published once, and a tag override is rejected.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create two packages that share a dependency.
let root = artifact {
	packages: {
		shared: {
			tangram.ts: '
				export let metadata = { tag: "shared/1.0.0" };
			'
		}
		a: {
			tangram.ts: '
				import shared from "shared" with { source: "../shared" };
				export let metadata = { tag: "a/1.0.0" };
			'
		}
		b: {
			tangram.ts: '
				import shared from "shared" with { source: "../shared" };
				export let metadata = { tag: "b/1.0.0" };
			'
		}
	}
}
let a = $root | path join "packages/a"
let b = $root | path join "packages/b"

# The dry run prints one plan that lists the shared dependency once.
let output = tg publish --dry-run $a $b | complete
success $output "the dry run should succeed"
let tags = $output.stdout | from json | get tag | sort
assert equal $tags [a/1.0.0 b/1.0.0 shared/1.0.0] "the plan should list each package once"

# A tag override with several paths fails.
let output = tg publish --tag other/1.0.0 $a $b | complete
failure $output "a tag override with several paths should fail"
assert ($output.stderr | str contains "single path")

# Publishing both paths tags both packages and the shared dependency on the local server and the remote.
let output = tg publish $a $b | complete
success $output "the publish should succeed"
for tag in [a/1.0.0 b/1.0.0 shared/1.0.0] {
	let local_target = tg tag get $tag | from json | get target.id
	let remote_target = tg --url $remote.url tag get $tag | from json | get target.id
	assert equal $remote_target $local_target $"the remote tag ($tag) should target the local package"
}

