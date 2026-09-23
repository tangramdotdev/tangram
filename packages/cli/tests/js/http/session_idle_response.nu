use ../../lib/test.nu *

# A server closing an idle HTTP/2 connection must not discard a buffered response.
let server = server spawn --config { http: { idle_timeout: 0.01 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			const object = await tg.blob("x".repeat(1024 * 1024));
			await object.store();
			const session = tg.host.http2.connect(tg.process.env.TANGRAM_URL as string);
			const stream = session.request({
				":path": `/objects/${object.id}`,
				accept: "application/json",
				authorization: `Bearer ${tg.process.env.TANGRAM_TOKEN}`,
			});
			stream.on("data", () => {
				// Simulate a busy JavaScript reader while the server idle timeout expires.
				const deadline = Date.now() + 25;
				while (Date.now() < deadline) {}
			});
			const response = await tg.Response.fromStream(stream);
			await response.json();
		}
	'
}

let output = tg build $path | complete
success $output 'the buffered response should survive an idle connection close'
