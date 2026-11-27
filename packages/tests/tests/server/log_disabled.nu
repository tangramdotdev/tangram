use ../../util/test.nu *

# A server run in the foreground without the `log` key leaves its output alone and writes no log, and `tangram server log` says so.

let local = server spawn
tg health | ignore

assert not ($local.directory | path join 'log' | path exists) 'the server should not create a log directory'

let output = tg -d $local.directory server log | complete
failure $output 'printing the log should fail without a log'
assert ($output.stderr | str contains 'the server has no log') $output.stderr
