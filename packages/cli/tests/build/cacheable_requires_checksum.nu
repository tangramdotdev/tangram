use ../../test.nu *

let server = spawn

def assert_cacheable_error [source: string] {
	let path = artifact {
		tangram.ts: $source,
	}

	let output = tg build $path | complete
	failure $output
	assert ($output.stderr | str contains "a build must be cacheable") "The error should mention cacheability."
}

assert_cacheable_error '
	export default async () => {
		return await tg.build(() => tg.file("Hello, World!")).network(true);
	};
'

assert_cacheable_error '
	export default async () => {
		return await tg.build({
			executable: "sh",
			args: ["-c", "true"],
			mounts: [{ source: "/tmp", target: "/work" }],
		});
	};
'

assert_cacheable_error '
	export default async () => {
		return await tg.build({
			executable: "sh",
			args: ["-c", "true"],
			stdin: "pipe",
		});
	};
'

assert_cacheable_error '
	export default async () => {
		return await tg.build({
			executable: "sh",
			args: ["-c", "true"],
			stdout: "inherit",
		});
	};
'

assert_cacheable_error '
	export default async () => {
		return await tg.build({
			executable: "sh",
			args: ["-c", "true"],
			stderr: "inherit",
		});
	};
'

assert_cacheable_error '
	export default async () => {
		return await tg.build({
			executable: "sh",
			args: ["-c", "true"],
			tty: true,
		});
	};
'

let checksum_path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.build(() => tg.file("Hello, World!"))
				.network(true)
				.checksum("none");
		};
	',
}

let checksum_output = tg build $checksum_path | complete
failure $checksum_output
assert not ($checksum_output.stderr | str contains "a build must be cacheable") "A checksum should bypass the cacheability guard."

let cli_path = artifact {
	tangram.ts: '
		export default () => tg.file("Hello, World!");
	',
}

let cli_output = tg build --network=true $cli_path | complete
failure $cli_output
assert ($cli_output.stderr | str contains "a build must be cacheable") "The CLI build command should enforce cacheability."
