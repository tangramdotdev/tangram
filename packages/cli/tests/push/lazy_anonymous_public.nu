use ../lib/test.nu *

# An anonymous lazy push makes its objects public, so a later anonymous push can rely on them.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote.url } },
}
let local_directory_source = server spawn --name local-directory-source --config {
	remotes: { default: { url: $remote.url } },
}

# An anonymous push stores a public file and blob on the remote.
let directory = tg --url $local_source.url put --no-tokens 'tg.directory({ "public.txt": tg.file("public") })' | referent node
tg --url $local_source.url index
let file = tg --url $local_source.url children $directory | from json | get 0

let output = tg --url $local_source.url --no-quiet push --lazy $file | complete
success $output "An anonymous push should succeed."
snapshot ($output.stderr | lines | where {|l| $l =~ '(transferred|skipped)'} | sort | str join "\n") '
	info transferred 2 objects, 51 B
'

tg --url $remote.url index

# A second anonymous client has only the directory structure, not the file or blob.
tg --url $local_source.url get --bytes $directory | tg --url $local_directory_source.url put --no-tokens --bytes --kind dir | referent node

# The later anonymous push relies on the public file subtree and transfers only the directory.
let output = tg --url $local_directory_source.url --no-quiet push --lazy $directory | complete
success $output "A later anonymous push should rely on the public file subtree."
snapshot ($output.stderr | lines | where {|l| $l =~ '(transferred|skipped)'} | sort | str join "\n") '
	info skipped 2 objects, 51 B
	info transferred 1 object, 62 B
'
