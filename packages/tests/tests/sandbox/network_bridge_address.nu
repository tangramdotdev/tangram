use ../../util/test.nu *

# A sandbox keeps its connection to its gateway while other networked sandboxes are destroyed.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux network namespaces'
}

let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten

# Accept a connection, then send a byte on it without receiving one first when signaled, so that the host does not refresh the sandbox's gateway.
let listen = '
import signal, socket
signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGUSR1])
with socket.socket() as listener:
    listener.bind(("0.0.0.0", 8080))
    listener.listen()
    print("ready", flush=True)
    connection, _ = listener.accept()
    with connection:
        connection.recv(4)
        connection.sendall(b"a")
        signal.sigwait([signal.SIGUSR1])
        connection.sendall(b"b")
        connection.recv(1)
'

# Connect to a published port, report that the sandbox replied, and then receive the byte that it sends.
let connect = '
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10) as connection:
    connection.sendall(b"ping")
    assert connection.recv(1) == b"a"
    open(sys.argv[2], "w").close()
    open(sys.argv[3]).close()
    connection.settimeout(5)
    assert connection.recv(1) == b"b"
'

let server = server spawn --root --config {
	runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
}

# Create other networked sandboxes, run a process in each so that it has joined the bridge.
let others = 0..7 | each {
	let other = tg sandbox create --no-tokens --no-ttl --network ...$mounts | referent node
	tg run --no-tokens $'--sandbox=($other)' --executable /usr/bin/python3 -- -c 'pass'
	$other
}

# Connect to a sandbox, so that it caches its gateway.
let directory = mktemp -d
let connected = $directory | path join connected
let resume = $directory | path join resume
^mkfifo $connected $resume
let sandbox = tg sandbox create --no-tokens --no-ttl --network --port 127.0.0.1::8080 ...$mounts | referent node
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /usr/bin/python3 -- -c $listen | referent node
let output = timeout 30s tg log --no-timeout --length 6 $process | complete
success $output 'the listener should start'
assert equal $output.stdout "ready\n"
let port = tg sandbox get $sandbox | from json | get data.network.ports.0 | split row ':' | get 1
let job = job spawn {
	let job_id = job id
	python3 -c $connect $port $connected $resume | complete | job send --tag $job_id 0
}
let output = timeout 30s cat $connected | complete
success $output 'the sandbox should be reachable'

# Destroy the other sandboxes.
for other in $others {
	tg sandbox destroy $other
	tg sandbox wait $other | ignore
}

# Send from the sandbox.
^sh -c ': > "$0"' $resume
tg signal --signal USR1 $process
let output = job recv --tag $job
success $output 'the sandbox should still reach its gateway'
let output = tg wait $process | from json
assert equal $output.exit 0
