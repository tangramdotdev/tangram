use ../../test.nu *

# Pushing a nested tag rejects a missing parent with ancestors=never and pulls it by default.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

tg group create parent
let file = tg put 'tg.file("data")' | str trim
tg tag put parent/tag $file

let output = tg push --ancestors=never parent/tag | complete
failure $output
assert ($output.stderr | str contains "the parent does not exist")

tg push parent/tag
let local_parent = tg group get parent | from json
let remote_parent = tg --url $remote.url group get parent | from json
assert equal $remote_parent.id $local_parent.id
