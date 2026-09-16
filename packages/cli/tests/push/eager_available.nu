use ../../test.nu *

# Repeating an eager push skips an available subtree without transferring any objects.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let directory = tg --url $local.url put 'tg.directory({ "a": tg.file("a"), "b": tg.file("b") })' | str trim
tg --url $local.url index

success (tg --url $local.url push $directory | complete)
tg --url $remote.url index

let output = tg --url $local.url --no-quiet push $directory | complete
success $output
assert ($output.stderr | str contains "skipped") "the available subtree should be skipped"
assert not ($output.stderr | str contains "transferred") "the available subtree should not be transferred"
