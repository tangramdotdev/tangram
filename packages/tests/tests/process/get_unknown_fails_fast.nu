use ../../util/test.nu *

# A request for an unknown process that is too old to be starting fails without waiting for the process's control service.

const lib = path self '../lib'
let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let mallory = tg login --verbose --name mallory | from json

# Create an ID with a timestamp an hour ago, which no process uses.
let script = "import sys, time, os; sys.path.insert(0, sys.argv[1]); from process_id import encode, uuidv7; print('pcs_00' + encode(uuidv7(int(time.time() * 1000) - 3600000, os.urandom(10))))"
let process = python3 -c $script $lib | str trim

# The request fails before the control service request would time out.
let start = date now
let output = tg --token $mallory.token get $process | complete
let elapsed = (date now) - $start
failure $output "the request for an unknown process should fail"
assert ($elapsed < 5sec) $"the request should fail quickly, but took ($elapsed)"
