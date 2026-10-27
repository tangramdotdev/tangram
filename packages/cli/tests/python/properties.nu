use ../lib/test.nu *

const script_path = path self 'properties.py'

let local = server spawn
success (^python $script_path | complete)
