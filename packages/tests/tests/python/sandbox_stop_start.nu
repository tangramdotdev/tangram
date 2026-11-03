use ../../util/test.nu *

const script_path = path self 'sandbox_stop_start.py'

let local = server spawn
success (^python $script_path | complete)
