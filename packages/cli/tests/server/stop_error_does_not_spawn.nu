use ../../test.nu *

# A failing `server stop` does not spawn a server. Printing an error acquires a client, and a client in auto mode spawns a server when it cannot connect.

let directory = mktemp -d
'not a pid' | save -f ($directory | path join 'lock')

# The harness sets client mode, which never spawns. Auto mode is the default a user gets.
let output = with-env { TANGRAM_MODE: auto } { tg -d $directory server stop | complete }
failure $output

# Record whether a server appeared, then stop it before asserting, so that a failure of this test does not leak a server the harness cannot reap.
let spawned = ($directory | path join 'socket') | path exists
if $spawned {
	tg -d $directory server stop | complete | ignore
}

assert not $spawned 'printing the error should not have spawned a server'
