use ../../test.nu *

# Putting a nested tag with a configured remote fails when the parent does not exist and -p was not passed.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let output = tg --url $local.url tag foo/bar | complete

failure $output "tagging should fail when the parent does not exist"
assert ($output.stderr | str contains "the parent does not exist")
