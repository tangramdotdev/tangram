use ../lib/test.nu *

const python_path = path self '../../../../.venv/bin/python'
const script_path = path self 'execution.py'

let local = server spawn
let output = ^$python_path $script_path | complete
success $output
assert equal ($output.stdout | str trim) 'python execution completed'
