use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export default async () => {
			return await tg.run("echo 'Hello, World!' > $OUTPUT", { checksum: "sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017" });
		};
	'#
}

let output = tg build $path | complete
failure $output
