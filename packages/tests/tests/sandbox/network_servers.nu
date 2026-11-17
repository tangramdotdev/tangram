use ../../util/test.nu *

# Several root servers on one host run bridge networked sandboxes with published ports at the same time, with either firewall.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux network namespaces'
}

let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let script = '
import socket, sys
with socket.socket() as listener:
    listener.bind(("0.0.0.0", 8080))
    listener.listen()
    print("ready", flush=True)
    connection, _ = listener.accept()
    with connection:
        connection.recv(4)
        connection.sendall(sys.argv[1].encode())
'

for firewall in [nft iptables] {
	# Create a networked sandbox with a published port on each server, and keep all of them running.
	let servers = [a b c] | each {|name|
		let server = server spawn --root --name $name --config {
			runner: { backend: { kind: builtin }, sandbox_pool_size: 0 },
			sandbox: { network: { firewall: $firewall } },
		}
		let sandbox = tg --url $server.url sandbox create --no-tokens --no-ttl --network --port 127.0.0.1::8080 ...$mounts | referent node
		let process = tg --url $server.url spawn --no-tokens $'--sandbox=($sandbox)' --executable /usr/bin/python3 -- -c $script $name | referent node
		let output = timeout 30s tg --url $server.url log --no-timeout --length 6 $process | complete
		success $output 'the listener should start'
		assert equal $output.stdout "ready\n"
		let port = tg --url $server.url sandbox get $sandbox | from json | get data.network.ports.0 | split row ':' | get 1
		{ name: $name, port: $port, process: $process, sandbox: $sandbox, server: $server }
	}

	# Reach every sandbox through its published port.
	for entry in $servers {
		let output = python3 -c 'import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as connection:
    connection.sendall(b"ping")
    print(connection.recv(16).decode())' $entry.port | complete
		success $output $'the published port of server ($entry.name) should be reachable with ($firewall)'
		assert equal ($output.stdout | str trim) $entry.name
		let output = tg --url $entry.server.url wait $entry.process | from json
		assert equal $output.exit 0
	}

	for entry in $servers {
		tg --url $entry.server.url sandbox destroy $entry.sandbox
		tg --url $entry.server.url sandbox wait $entry.sandbox | ignore
		server stop $entry.server
	}
}
