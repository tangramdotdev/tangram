use ../../util/test.nu *

# A failed warm configuration releases its resources and published port before cold fallback.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux namespaces'
}
if (^nproc | str trim | into int) < 2 {
	skip_test 'this test requires at least two CPUs'
}

def check_sandbox [sandbox: string, port: string] {
	let script = '
import os, socket
assert open("/target/message").read() == "hello\n"
assert socket.gethostname() == "configured"
assert len(os.sched_getaffinity(0)) == 1
with socket.socket() as listener:
    listener.bind(("0.0.0.0", 8080))
    listener.listen()
    print("ready", flush=True)
    connection, _ = listener.accept()
    with connection:
        connection.recv(4)
        connection.sendall(b"hello")
'
	let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /usr/bin/python3 -- -c $script | referent node
	let output = timeout 30s tg log --no-timeout --length 6 $process | complete
	success $output 'the configured sandbox should start its listener'
	assert equal $output.stdout "ready\n"
	let output = python3 -c 'import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as connection:
    connection.sendall(b"ping")
    print(connection.recv(5).decode())' $port | complete
	success $output 'the sandbox should publish its port after failed warm configuration'
	assert equal ($output.stdout | str trim) hello
	let output = tg wait $process | from json
	assert equal $output.exit 0
	tg sandbox destroy $sandbox
	tg sandbox wait $sandbox | ignore
}

let backends = if (which bwrap | is-empty) { [builtin] } else { [builtin bubblewrap] }
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
for backend in $backends {
	for root in [false true] {
		let config = {
			advanced: { checkpoints: true },
			runner: { backend: { kind: $backend, storage_limits: $root }, sandbox_pool_size: 1, storage: 201326592 },
			sandbox: { network: { ip_ranges: ['172.18.251.4-172.18.251.7'] } },
			scheduler: { default_cpu: 2, default_memory: 134217728, default_storage: 67108864 },
		}
		let local = if $root { server spawn --root --config $config } else { server spawn --config $config }
		let source = if $root {
			project_quota_directory | path join source
		} else {
			$'source-($backend)' | path expand
		}
		let saved = $'($source)-saved'
		mkdir $source
		"hello\n" | save ($source | path join message)
		let port = python3 -c 'import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])' | str trim
		let args = [--no-tokens --no-time-to-live --cpu 1 --memory 268435456 --storage-min 134217728 --storage 134217728 --hostname configured --network --port $'127.0.0.1:($port):8080' --mount $'($source):/target,ro'] | append $mounts

		# Remove the source before configuration and restore it after the mount failure, so cold fallback can succeed.
		let configure = checkpoint watch runner.sandbox.pool.configure
		let reject = checkpoint watch runner.sandbox.pool.reject
		let job = job spawn {
			let job_id = job id
			tg sandbox create ...$args | job send --tag $job_id 0
		}
		checkpoint wait $configure | ignore
		mv $source $saved
		checkpoint unwatch $configure
		checkpoint wait $reject | ignore
		mv $saved $source
		checkpoint unwatch $reject
		let sandbox = job recv --tag $job | referent node
		check_sandbox $sandbox $port

		# Claim the replacement warm sandbox with the same published port and storage reservation.
		let take = checkpoint watch runner.sandbox.pool.take
		let job = job spawn {
			let job_id = job id
			tg sandbox create ...$args | job send --tag $job_id 0
		}
		checkpoint pass $take | ignore
		checkpoint unwatch $take
		let sandbox = job recv --tag $job | referent node
		check_sandbox $sandbox $port
		server stop $local
	}
}
