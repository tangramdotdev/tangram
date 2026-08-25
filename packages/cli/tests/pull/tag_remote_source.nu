use ../../test.nu *

# Pulling a tag with a specified remote fetches the object from that remote even when another remote has a conflicting tag of the same name.

let other = server spawn --cloud --name other
let source = server spawn --cloud --name source
let local = server spawn --name local

let other_id = tg --url $other.url put 'tg.file("from the other remote")' | str trim
tg --url $other.url tag -p conflict/1.0.0 $other_id

let source_id = tg --url $source.url put 'tg.file("from the source remote")' | str trim
tg --url $source.url tag -p conflict/1.0.0 $source_id

tg --url $local.url remote put other $other.url
tg --url $local.url remote put source $source.url

let output = tg --url $local.url pull --remote=source --group-children conflict | complete
success $output

let output = tg --url $local.url object get --local $source_id --pretty | complete
success $output
