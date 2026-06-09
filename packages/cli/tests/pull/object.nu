use ../../test.nu *

# Pulling an object by id fetches it from the remote and makes it present locally.

let remote = spawn --cloud --name remote
let local = spawn --name local
tg remote put default $remote.url

let id = tg --url $remote.url put 'tg.file("pulled object")' | str trim

# The object is absent locally before the pull.
let before = tg object get --local $id | complete
failure $before "the object should be absent locally before the pull"

tg pull $id

# The object is present locally after the pull.
let after = tg object get --local $id | complete
success $after "the object should be present locally after the pull"
