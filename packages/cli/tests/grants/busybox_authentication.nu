use ../../test.nu *

# A user can build a package that depends on busybox under authentication.

let server = spawn --busybox --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async function () {
			return tg.run`mkdir -p $TANGRAM_OUTPUT && echo hello > $TANGRAM_OUTPUT/message.txt`.env(tg.build(busybox));
		}
	',
}

success (tg --token $alice.token build $path | complete) "using busybox under authentication must work"
