use ../../test.nu *

# Pushing a nested tag does not also push its parent group.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url

tg group create parent
let file = tg put 'tg.file("data")' | str trim
tg tag put parent/tag $file

let output = tg push parent/tag | complete
failure $output
assert ($output.stderr | str contains "the parent does not exist")
failure (tg --url $remote.url group get parent | complete)
