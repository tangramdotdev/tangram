use ../../test.nu *

# This test exercises the container sandbox networking modes by running a
# build that depends on busybox. The busybox build performs a download, so
# the outcome depends on the sandbox's network access.

let source = '
	import busybox from "busybox";
	export default () => tg.build`echo hello > ${tg.output}`.env(tg.build(busybox));
'

# The build should fail when the container has no network because the
# busybox download cannot reach the internet.
let server = spawn --busybox --config {
	sandbox: {
		isolation: {
			kind: 'container',
			net: { kind: 'none' },
		},
	},
}
let path = artifact { tangram.ts: $source }
let output = tg -u $server.url build $path | complete
failure $output 'the build should fail without network access'

# The build should succeed when the container shares the host's network.
let server = spawn --busybox --config {
	sandbox: {
		isolation: {
			kind: 'container',
			net: { kind: 'host' },
		},
	},
}
let path = artifact { tangram.ts: $source }
let output = tg -u $server.url build $path | complete
success $output

# The build should succeed when the container shares the host's network.
let server = spawn --busybox --config {
	sandbox: {
		isolation: {
			kind: 'container',
			net: { kind: 'bridge' },
		},
	},
}
let path = artifact { tangram.ts: $source }
let output = tg -u $server.url build $path | complete
success $output
