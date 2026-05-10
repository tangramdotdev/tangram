use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local

let id = tg -u $remote.url put 'tg.file("Hello from the remote tag!")' | str trim
tg -u $remote.url tag remote-only/1.0.0 $id
tg -u $local.url remote put source $remote.url

let output = tg -u $local.url pull --remote=source remote-only/1.0.0 | complete
success $output

let output = tg -u $local.url get remote-only/1.0.0 --pretty | complete
success $output
