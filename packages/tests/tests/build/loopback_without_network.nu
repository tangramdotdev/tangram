use ../../util/test.nu *

# A process without network access can reach a service it starts on the loopback interface.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.build`
			cd "$(mktemp -d)"
			mkdir www
			echo hello > www/index.html
			httpd -f -p 127.0.0.1:8080 -h www &
			server=$!
			for i in $(seq 50); do
				if wget -q -O ${tg.output} http://127.0.0.1:8080/index.html; then
					kill $server
					exit 0
				fi
				sleep 0.1
			done
			kill $server
			exit 1
		`.env(tg.build(busybox));
	',
}

for kind in [builtin bubblewrap] {
	let backend = match $kind {
		builtin => { kind: builtin },
		bubblewrap => {
			if (which bwrap | is-empty) { continue }
			{ kind: bubblewrap, path: (which bwrap | first | get path) }
		},
	}
	let local = server spawn --busybox --directory (mktemp --directory) --config { runner: { backend: $backend } }
	let output = tg build $path | complete
	success $output $"($kind): the build should reach its loopback service"
	assert equal (tg read ($output.stdout | str trim) | str trim) hello
	server stop $local
}
