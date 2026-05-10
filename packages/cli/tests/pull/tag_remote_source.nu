use ../../test.nu *

let other = spawn --cloud -n other
let source = spawn --cloud -n source
let local = spawn -n local

let other_id = tg -u $other.url put 'tg.file("from the other remote")' | str trim
tg -u $other.url tag conflict/1.0.0 $other_id

let source_id = tg -u $source.url put 'tg.file("from the source remote")' | str trim
tg -u $source.url tag conflict/1.0.0 $source_id

tg -u $local.url remote put other $other.url
tg -u $local.url remote put source $source.url

let output = tg -u $local.url pull --remote=source conflict/1.0.0 | complete
success $output

let output = tg -u $local.url object get --local $source_id --pretty | complete
success $output
