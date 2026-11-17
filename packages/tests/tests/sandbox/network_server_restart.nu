use ../../util/test.nu *

# A root server that is killed and restarted in the same directory frees the host ports that its sandboxes published, without disturbing a port that another server's sandbox publishes, with either firewall.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux network namespaces'
}

const script = '
import socket, sys
with socket.socket() as listener:
    listener.bind(("0.0.0.0", 8080))
    listener.listen()
    print("ready", flush=True)
    while True:
        connection, _ = listener.accept()
        with connection:
            connection.recv(4)
            connection.sendall(sys.argv[1].encode())
'

# Create a sandbox that publishes a port and serves its name on it.
def serve [server: record, name: string, mounts: list<string>] {
	let sandbox = tg --url $server.url sandbox create --no-tokens --no-ttl --network --port 127.0.0.1::8080 ...$mounts | referent node
	let process = tg --url $server.url spawn --no-tokens $'--sandbox=($sandbox)' --executable /usr/bin/python3 -- -c $script $name | referent node
	let output = timeout 30s tg --url $server.url log --no-timeout --length 6 $process | complete
	success $output 'the listener should start'
	assert equal $output.stdout "ready\n"
	let port = tg --url $server.url sandbox get $sandbox | from json | get data.network.ports.0 | split row ':' | get 1

	{ port: $port, process: $process, sandbox: $sandbox }
}

# Connect to a port on the host and print the reply.
def request [port: string] {
	python3 -c 'import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as connection:
    connection.sendall(b"ping")
    print(connection.recv(16).decode())' $port | complete
}

let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
for firewall in [nft iptables] {
	let config = {
		runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
		sandbox: { network: { firewall: $firewall } },
	}
	let a = server spawn --root --name a --config $config
	let b = server spawn --root --name b --config $config
	let served_a = serve $a a $mounts
	let served_b = serve $b b $mounts
	let output = request $served_a.port
	success $output 'the published port of the first server should be reachable'
	assert equal ($output.stdout | str trim) a

	# Kill the first server, then restart it in the same directory and publish a port from a new sandbox.
	let pid = open ($a.directory | path join 'lock') | str trim | into int
	sudo -n kill -9 $pid
	wait_for_exit $pid
	let a = server start $a
	let restarted = serve $a restarted $mounts
	let output = request $restarted.port
	success $output 'the published port of the restarted server should be reachable'
	assert equal ($output.stdout | str trim) restarted

	# Check that the host can serve on the port that the killed server's sandbox published.
	let output = python3 -c 'import socket, sys
with socket.socket() as listener:
    listener.bind(("127.0.0.1", int(sys.argv[1])))
    listener.listen()
    listener.settimeout(5)
    with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as connection:
        accepted, _ = listener.accept()
        with accepted:
            accepted.sendall(b"host")
        print(connection.recv(16).decode())' $served_a.port | complete
	success $output 'the port of the killed server should be free on the host'
	assert equal ($output.stdout | str trim) host

	# Check that the other server's published port still reaches its sandbox.
	let output = request $served_b.port
	success $output 'the published port of the other server should remain reachable'
	assert equal ($output.stdout | str trim) b

	for entry in [[server sandbox]; [$a $restarted.sandbox] [$b $served_b.sandbox]] {
		tg --url $entry.server.url sandbox destroy $entry.sandbox
		tg --url $entry.server.url sandbox wait $entry.sandbox | ignore
	}
	server stop $a
	server stop $b
}
