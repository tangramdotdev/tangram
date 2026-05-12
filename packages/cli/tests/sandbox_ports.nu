use ../test.nu *

let server = spawn

let sandbox = (tg sandbox create -p 80 -p 127.0.0.1::53/udp | str trim)
assert ($sandbox | str starts-with "sbx_")

let get = tg sandbox get $sandbox | from json
assert ($get.network == true)
assert (($get.ports | where { |port| $port =~ '^[0-9]+:80$' } | length) == 1)
assert (($get.ports | where { |port| $port =~ '^127\.0\.0\.1:[0-9]+:53/udp$' } | length) == 1)

let list = tg sandbox list | from json
let listed = ($list | where id == $sandbox | first)
assert equal $listed.ports $get.ports

let output = tg sandbox create --no-network -p 8080:80 | complete
failure $output
assert ($output.stderr | str contains 'ports require networking') "the error should mention networking"

let output = tg sandbox create --network=host -p 8080:80 | complete
failure $output
assert ($output.stderr | str contains 'ports are not supported with host networking') "the error should mention host networking"

let path = artifact {
	script: (file -x '
		#!/bin/sh
		exit 0
	')
}
let executable = tg checkin ($path | path join "script") | str trim

let output = tg spawn $"--sandbox=($sandbox)" -p 8080:80 --executable $executable | complete
failure $output
assert ($output.stderr | str contains 'sandbox options are not supported for existing sandboxes') "the error should mention the existing sandbox"

tg sandbox delete $sandbox
