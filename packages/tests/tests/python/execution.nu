use ../../util/test.nu *

const script_path = path self 'execution.py'

let local = server spawn
let output = ^python $script_path | complete
success $output
assert equal ($output.stdout | str trim) 'python execution completed'
