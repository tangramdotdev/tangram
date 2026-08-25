use ../../test.nu *

# A successful push invalidates cached reads for the destination remote.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url
tg group create cached

failure (tg get --remote cached | complete)
tg push cached
success (tg get --remote cached | complete)
