use ../../util/test.nu *

# A warm sandbox applies custom mounts without covering them or leaving its handoff files in them.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux namespaces'
}
let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: builtin }, sandbox_pool_size: 1 },
}
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
def create [args: list<string>] {
	let checkpoint = checkpoint watch runner.sandbox.pool.take
	let job = job spawn {
		let job_id = job id
		tg sandbox create --no-tokens --no-time-to-live ...$args | job send --tag $job_id 0
	}
	checkpoint pass $checkpoint | ignore
	checkpoint unwatch $checkpoint
	job recv --tag $job | referent node
}

# Keep the resolver file of a mounted /etc when the sandbox has no network.
let etc = 'etc' | path expand
mkdir $etc
"nameserver 9.9.9.9\n" | save ($etc | path join resolv.conf)
let sandbox = create ($mounts | append [--mount $'($etc):/etc'])
let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/cat -- /etc/resolv.conf | complete
success $output 'the mounted resolver file should be readable'
assert equal ($output.stdout | str trim) 'nameserver 9.9.9.9'
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore

# Keep a custom mount nested in a mandatory mount, and keep the namespace socket out of the parent mount.
let opt = 'opt' | path expand
let nested = 'nested' | path expand
mkdir $opt
mkdir $nested
"marker\n" | save ($nested | path join marker)
let sandbox = create ($mounts | append [--mount $'($opt):/opt' --mount $'($nested):/opt/tangram/output/extra'])
let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/cat -- /opt/tangram/output/extra/marker | complete
success $output 'the nested mount should be visible'
assert equal ($output.stdout | str trim) marker
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore
assert (not ($opt | path join tangram/namespaces.sock | path exists)) 'the namespace socket should not be left in the mount source'
