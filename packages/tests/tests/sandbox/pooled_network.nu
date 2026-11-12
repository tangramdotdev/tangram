use ../../util/test.nu *

# Root runners attach a bridge interface and publish ports when claiming a warm sandbox.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux network namespaces'
}
let local = server spawn --root --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: builtin }, sandbox_pool_size: 1 },
}
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let checkpoint = checkpoint watch runner.sandbox.pool.take
let job = job spawn {
	let job_id = job id
	tg sandbox create --no-tokens --no-time-to-live --network --port 127.0.0.1::8080 ...$mounts | job send --tag $job_id 0
}
checkpoint pass $checkpoint | ignore
checkpoint unwatch $checkpoint
let sandbox = job recv --tag $job | referent node
let script = '
import socket
assert "eth0" in [name for _, name in socket.if_nameindex()]
assert open("/etc/resolv.conf").read().strip()
with socket.socket() as listener:
    listener.bind(("0.0.0.0", 8080))
    listener.listen()
    print("ready", flush=True)
    connection, _ = listener.accept()
    with connection:
        connection.recv(4)
        connection.sendall(b"ok")
'
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /usr/bin/python3 -- -c $script | referent node
let output = timeout 30s tg log --no-timeout --length 6 $process | complete
success $output 'the network listener should start'
assert equal $output.stdout "ready\n"
let port = tg sandbox get $sandbox | from json | get data.network.ports.0 | split row ':' | get 1
let output = python3 -c 'import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as connection:
    connection.sendall(b"ping")
    print(connection.recv(2).decode())' $port | complete
success $output 'the published port should be reachable'
assert equal ($output.stdout | str trim) 'ok'
let output = tg wait $process | from json
assert equal $output.exit 0
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore
