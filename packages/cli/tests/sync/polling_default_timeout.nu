use ../../test.nu *

let source = server spawn --name source
let local = server spawn --name local
let id = tg --url $source.url put 'tg.blob("absent")' | str trim
let started = date now
let output = tg --url $local.url get --local --bytes $id | complete
let elapsed = (date now) - $started
failure $output
assert ($elapsed >= 800ms and $elapsed < 3sec) 'the default availability timeout must be one second'
