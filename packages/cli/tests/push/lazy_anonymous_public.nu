use ../../test.nu *

# An anonymous lazy push makes its objects public, so a later anonymous push can rely on them.

let remote = spawn --cloud --name remote --config { authentication: true }

let source = spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let directory_source = spawn --name directory-source --config {
	remotes: { default: { url: $remote.url } },
}

# An anonymous push stores a public file and blob on the remote.
let directory = tg --url $source.url put 'tg.directory({ "public.txt": tg.file("public") })' | str trim
tg --url $source.url index
let file = tg --url $source.url children $directory | from json | get 0

let output = tg --url $source.url --no-quiet push --lazy $file | complete
success $output "An anonymous push should succeed."
assert ($output.stderr | str contains "transferred 0 processes, 2 objects") "The anonymous push should transfer the file and blob."

tg --url $remote.url index

# A second anonymous client has only the directory structure, not the file or blob.
tg --url $source.url get --bytes $directory | tg --url $directory_source.url put --bytes --kind dir

# The later anonymous push relies on the public file subtree and transfers only the directory.
let output = tg --url $directory_source.url --no-quiet push --lazy $directory | complete
success $output "A later anonymous push should rely on the public file subtree."
assert ($output.stderr | str contains "skipped 0 processes, 2 objects") "The later anonymous push should skip the public file and blob."
assert ($output.stderr | str contains "transferred 0 processes, 1 objects") "The later anonymous push should transfer only the directory."
