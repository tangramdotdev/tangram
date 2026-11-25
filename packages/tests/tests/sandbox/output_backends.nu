use ../../util/test.nu *

# Every runner backend checks in the output of a process in a sandbox with a ttl of zero with the files that the process wrote, and a writer that a process leaves with a writable file descriptor to its output cannot change the checked-in artifact.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
for kind in [builtin bubblewrap gvisor-systrap gvisor-kvm] {
	let backend = match $kind {
		builtin => { kind: builtin },
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			{ kind: bubblewrap, path: (which bwrap | first | get path) }
		},
		_ => {
			if (which runsc | is-empty) { continue }
			let platform = if $kind == gvisor-kvm { 'kvm' } else { 'systrap' }
			if $platform == kvm and ((^test -r /dev/kvm -a -w /dev/kvm | complete).exit_code != 0) { continue }
			{ kind: gvisor, path: (which runsc | first | get path), platform: $platform }
		},
	}
	let local = server spawn --config { runner: { backend: $backend }, vfs: false }
	let path = artifact {
		tangram.ts: '
			export const build = () => tg.command`
				mkdir -p ${tg.output}/directory
				echo hello > ${tg.output}/directory/file
				echo "echo hi" > ${tg.output}/script
				chmod +x ${tg.output}/script
				ln -s directory/file ${tg.output}/link
			`.env({ PATH: "/usr/bin:/bin" });
			export const write = () => tg.command`
				mkdir -p ${tg.output}
				echo original > ${tg.output}/file.txt
				mkfifo /tmp/done /tmp/go /tmp/ready
				setsid sh -c "exec 3>>${tg.output}/file.txt; echo ready > /tmp/ready; read x < /tmp/go; echo tampered >&3; exec 3>&-; echo done > /tmp/done" </dev/null >/dev/null 2>&1 &
				read x < /tmp/ready
			`.env({ PATH: "/usr/bin:/bin" });
			export const release = () => tg.command`
				echo go > /tmp/go
				read x < /tmp/done
			`.env({ PATH: "/usr/bin:/bin" });
		'
	}

	# Run the processes with the host's programs.
	let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten

	# The output of a process in a sandbox with a ttl of zero has the files that the process wrote.
	let command = tg build --no-tokens $'($path)#build' | str trim
	let output = tg run --no-tokens --sandbox ...$mounts $command | complete
	success $output $"($kind): the process should succeed"
	let checkout = mktemp --directory | path join checkout
	tg checkout ($output.stdout | str trim | lines | last) --path $checkout
	assert equal (open --raw ($checkout | path join directory file)) "hello\n" $"($kind): the file should have the build's contents"
	assert (ls -l ($checkout | path join script) | get 0.mode | str contains 'x') $"($kind): the script should be executable"
	assert equal (ls -l ($checkout | path join link) | get 0.target) 'directory/file' $"($kind): the symlink should have the build's target"

	# Run a process that leaves a writer in a sandbox that outlives it, let the writer write, and wait for it to finish.
	let command = tg build --no-tokens $'($path)#write' | str trim
	let release = tg build --no-tokens $'($path)#release' | str trim
	let sandbox = tg sandbox create --wait --no-tokens --no-time-to-live --no-time-to-stop ...$mounts | referent node
	let output = tg run --no-tokens $'--sandbox=($sandbox)' $command | complete
	success $output $"($kind): the writing process should succeed"
	let id = $output.stdout | str trim | lines | last
	success (tg run --no-tokens $'--sandbox=($sandbox)' $release | complete) $"($kind): the writer should finish"

	# The artifact's contents are unchanged.
	let checkout = tg checkout $id | str trim
	assert equal (open --raw ($checkout | path join file.txt)) "original\n" $"($kind): the artifact should be unchanged"
	tg sandbox destroy $sandbox
	server stop $local
}
