use ../../test.nu *

# A local process whose output is checked in must succeed: unset checkin options are omitted from the request, not sent as null, which the server's non-optional fields would reject.

let server = spawn

let path = artifact {
	tangram.ts: '
		export const inner = () => {};
		export default () => tg.run(inner);
	',
}

success (tg run $path | complete) "a local process output checkin must succeed with unset options omitted"
