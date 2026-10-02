use ../lib/test.nu *

# A process reference preserves command arguments and appends the new CLI arguments.

let local = server spawn
let path = artifact (file --executable '#!/bin/sh
printf "%s:%s:%s\n" "$VALUE" "$1" "$2"
')
let original = tg spawn --env-string VALUE=original --arg-string first $path | str trim
tg wait $original | ignore
let output = tg run --env-string VALUE=run --arg-string second $original | complete
success $output
assert equal ($output.stdout | str trim) 'run:first:second'

# A tag resolving to a process is accepted as well.
tg tag put previous $original
let output = tg run previous -- tagged | complete
success $output
assert equal ($output.stdout | str trim) 'original:first:tagged'
