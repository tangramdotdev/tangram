use ../lib/test.nu *

# Bridge networking permits communication within a sandbox while isolating peer sandboxes.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let module = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.command({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox));
	',
}

def test_backend [firewall: string] {
	let local = server spawn --busybox --config {
		runner: { sandbox_pool_size: 0 },
		sandbox: {
			network: {
				firewall: $firewall,
				ip_ranges: ['172.18.250.4-172.18.250.5'],
			},
		},
	}
	let listener_sandbox = tg sandbox create --no-tokens --network --port 127.0.0.1::8080 | referent node
	let client_sandbox = tg sandbox create --no-tokens --network | referent node
	let address_command = tg build $module --arg-string r#'ip -4 -o addr show scope global | awk '{ print $4 }' | cut -d/ -f1'# | str trim
	let listener_ip = tg run --no-tokens $'--sandbox=($listener_sandbox)' $address_command | str trim
	let listener_command = tg build $module --arg-string r#'echo ready; while :; do printf 'HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok' | nc -l -p 8080; done'# | str trim
	let listener = tg spawn --no-tokens $'--sandbox=($listener_sandbox)' $listener_command | referent node
	wait_until {
		(tg log $listener | complete).stdout | str contains 'ready'
	} 'the listener should start'

	let client_command = tg build $module --arg-string $'nc -w 2 ($listener_ip) 8080' | str trim
	let output = tg run --no-tokens $'--sandbox=($listener_sandbox)' $client_command | complete
	success $output 'a process should reach a listener in its own sandbox'
	assert ($output.stdout | str ends-with 'ok')

	let output = timeout 5s tg run --no-tokens $'--sandbox=($client_sandbox)' $client_command | complete
	failure $output 'a process should not reach a listener in a peer sandbox'
	let port = tg sandbox get $listener_sandbox | from json | get data.network.ports.0 | split row ':' | get 1
	let published_command = tg build $module --arg-string $'nc -w 2 127.0.0.1 ($port)' | str trim
	let output = timeout 5s tg run --no-tokens $published_command | complete
	success $output 'a published port should remain reachable from the host'
	assert ($output.stdout | str ends-with 'ok')

	tg signal --signal KILL $listener | ignore
	tg sandbox destroy $listener_sandbox
	tg sandbox destroy $client_sandbox
	server stop $local
}

for firewall in [nft iptables] {
	test_backend $firewall
}
