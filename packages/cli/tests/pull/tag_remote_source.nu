use ../lib/test.nu *

# Pulling a tag with a specified remote fetches the object from that remote even when another remote has a conflicting tag of the same name.

let local_other = server spawn --cloud --name local-other
let local_source = server spawn --cloud --name local-source
let local = server spawn --name local

let other_id = tg --url $local_other.url put --no-tokens 'tg.file("from the other remote")' | referent node
tg --url $local_other.url tag -p conflict/1.0.0 $other_id

let source_id = tg --url $local_source.url put --no-tokens 'tg.file("from the source remote")' | referent node
tg --url $local_source.url tag -p conflict/1.0.0 $source_id

tg --url $local.url remote put other $local_other.url
tg --url $local.url remote put source $local_source.url

let output = tg --url $local.url pull --remote=source --group-children conflict | complete
success $output

let output = tg --url $local.url object get --local $source_id --no-tokens --pretty | complete
success $output
