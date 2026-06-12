use ../../../test.nu *

# A process stdin writer's writeAll method writes all input and closes the stream, read back through the stdout reader's text method.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`read line; echo "got:$line"`.stdin("pipe").stdout("pipe").sandbox();
			let [, text] = await Promise.all([
				process.stdin.writeAll(tg.encoding.utf8.encode("hello\n")),
				process.stdout.text(),
			]);
			await process.wait();
			return text;
		};
	'
}

let output = tg build $path
snapshot $output '"got:hello\n"'
