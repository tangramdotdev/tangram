use ../lib/test.nu *

# A tokenless local miss fails without waiting for a polling timeout.

let local_source = server spawn --name local-source
let local = server spawn --name local
let id = tg --url $local_source.url put 'tg.blob("absent")' | str trim
let started = date now
let output = tg --url $local.url get --local --bytes $id | complete
let elapsed = (date now) - $started
failure $output
assert ($elapsed < 800ms) 'a tokenless miss must not wait for a polling timeout'
