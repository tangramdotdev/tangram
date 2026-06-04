use ../../test.nu *

# A descendant tag can be fetched and cached independently of the siblings already cached under the same parent.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
let old = tg --url $remote.url get $id | str trim

# Cache one deep descendant.
tg --url $remote.url tag put "a/c/e/f/g" $id
tg --url $local.url resolve "a/c/e/f/g" | ignore

# Add a sibling branch on the remote and fetch it.
tg --url $remote.url tag put "a/c/e/i/j" $id
let e = tg --url $local.url resolve "a/c/e/i/j" | str trim
assert equal $e $old "the new descendant should resolve through the local server"
