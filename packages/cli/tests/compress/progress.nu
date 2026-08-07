use ../../test.nu *

# The builtin reports progress while compressing an input.

let directory = mktemp -d
let input = $directory | path join 'input'
let output = $directory | path join 'output.gz'
'contents' | save $input

let result = tg builtin compress --format gz --input $input --output $output | complete
success $result
assert ($result.stderr | str contains 'compressing') "the builtin should report progress"
assert ($result.stderr | str contains 'finished compressing') "the builtin should report completion"
assert ($output | path exists) "the builtin should write the output"
