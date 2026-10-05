use ../lib/test.nu *

const python_path = path self '../../../../.venv/bin/python'
const script_path = path self 'properties.py'

let local = server spawn
success (^$python_path $script_path | complete)
