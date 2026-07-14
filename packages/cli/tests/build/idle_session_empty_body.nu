use ../../test.nu *

# A request sent after the connection's idle timeout must not send an empty body.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.client.write("hello");
			await tg.sleep(35);
			let id = await tg.client.write("world");
			let text = await tg.Blob.withId(id).text;
			return `second write round tripped as ${JSON.stringify(text)}`;
		}
	'
}

let output = tg build $path
snapshot $output '"second write round tripped as \"world\""'
