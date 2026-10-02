use ../lib/test.nu *

# Force cleaning stops the server and removes the entire directory.

let dir = mktemp --directory
let local = server spawn --directory $dir

tg put --no-tokens 'tg.file("force")' | referent node

let output = tg -d $dir clean --force | complete
success $output
assert (not ($dir | path exists)) "the directory should be removed"
