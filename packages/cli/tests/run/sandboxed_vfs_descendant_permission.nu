use ../lib/test.nu *

# The sandbox principal can read a command input's descendants without a direct artifact token.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires the per-sandbox Linux VFS'
}

for remote_runner in [false true] {
	let root_token = random chars
	let server = if $remote_runner {
		server spawn --name remote --busybox --config {
			authentication: { root: { token: $root_token } },
			roles: [api indexer scheduler],
		}
	} else {
		server spawn --busybox --config { vfs: true }
	}
	let runner = if $remote_runner {
		let created = tg --url $server.url --token $root_token runner create | from json
		server spawn --name runner --config {
			remotes: { default: { token: $created.token.token, url: $server.url } },
			roles: [indexer runner],
			runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
			vfs: true,
		}
	} else { null }

	let path = artifact {
		tangram.ts: '
			import busybox from "busybox";
			export default async function () {
				const child = tg.file("descendant contents");
				const directory = tg.directory({ child });
				const path = `/opt/tangram/store/${await child.id()}`;
				return await tg.run`test -d ${directory}; cat "${path}"`
					.env(tg.build(busybox))
					.sandbox();
			}
		',
	}
	let output = if $remote_runner {
		tg --url $server.url --token $root_token run $path | complete
	} else {
		tg --url $server.url run $path | complete
	}
	success $output 'the sandbox should read the descendant artifact through its process grant'
	assert equal ($output.stdout | str trim) 'descendant contents'
}
