use ../../../test.nu *

# Closing an HTTP/2 session gracefully must let an active response finish.
let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const session = tg.host.http2.connect(tg.process.env.TANGRAM_URL as string);
			const stream = session.request({
				":path": "/health?fields=version",
				authorization: `Bearer ${tg.process.env.TANGRAM_TOKEN}`,
			});
			const response = await tg.Response.fromStream(stream);
			await session.close();
			await response.json();
		}
	'
}

let output = tg build $path | complete
success $output 'closing the session should let the active response finish'
