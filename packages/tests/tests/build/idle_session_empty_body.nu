use ../../lib/test.nu *

# A request sent after the connection's idle timeout must not send an empty body.

let local = server spawn --config { http: { idle_timeout: 0.25 } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.client.write("hello");
			await tg.sleep(0.5);
			let id = await tg.client.write("world");
			let text = await tg.Blob.withId(id).text;
			return `second write round tripped as ${JSON.stringify(text)}`;
		}
	'
}

let output = tg build $path
snapshot $output '"second write round tripped as \"world\""'
