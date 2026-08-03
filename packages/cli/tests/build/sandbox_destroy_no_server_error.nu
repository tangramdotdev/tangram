use ../../test.nu *

# A build that succeeds logs no error on the server. Destroying a sandbox kills
# the sandbox process, which closes the socket under the server's HTTP client
# and fails its connection with a broken pipe.

let server = spawn --config { tracing: { stderr_format: 'json' } }

let path = artifact {
	tangram.ts: '
		export default async () => {
			return "hello";
		};
	'
}

tg build $path | ignore

snapshot (server_errors $server) ''
