use ../../test.nu *

# Putting a nested tag with a configured remote fails promptly when the parent does not exist and -p was not passed.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let output = timeout 5s tg --url $local.url tag foo/bar | complete

assert ($output.exit_code != 124) "tagging should not hang while resolving the missing parent"
failure $output "tagging should fail when the parent does not exist"
assert ($output.stderr | str contains "the parent does not exist")
