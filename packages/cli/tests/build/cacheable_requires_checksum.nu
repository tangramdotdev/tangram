use ../../test.nu *

# Builds requiring non-reproducible features such as network access, mounts, pipes, inherited stdio, or a tty fail unless a checksum is provided, and the CLI build command enforces the same cacheability guard.

let server = spawn

def assert_cacheable_error [source: string] {
	let path = artifact {
		tangram.ts: $source,
	}

	let output = tg build $path | complete
	failure $output
	let relevant = $output.stderr | lines | where {|l| $l =~ 'a build must be cacheable'} | sort
	snapshot --normalize-ids --redact $path $relevant '
		   ·            ╰── a build must be cacheable
		-> a build must be cacheable

	'
}

assert_cacheable_error '
	export default async function () {
		return await tg.build(() => tg.file("Hello, World!")).network();
	}
'

assert_cacheable_error '
	export default async function () {
		return await tg.build`true`.mount({ source: "/tmp", target: "/work" });
	}
'

assert_cacheable_error '
	export default async function () {
		return await tg.build`true`.stdin("pipe");
	}
'

assert_cacheable_error '
	export default async function () {
		return await tg.build`true`.stdout("inherit");
	}
'

assert_cacheable_error '
	export default async function () {
		return await tg.build`true`.stderr("inherit");
	}
'

assert_cacheable_error '
	export default async function () {
		return await tg.build`true`.tty(true);
	}
'

let checksum_path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(() => tg.file("Hello, World!"))
				.network(true)
				.checksum("none");
		}
	',
}

let checksum_output = tg build $checksum_path | complete
failure $checksum_output
snapshot --normalize-ids --redact $checksum_path $checksum_output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> failed to deserialize the request body
	-> invalid algorithm at line 1 column 18

'

let cli_path = artifact {
	tangram.ts: '
		export default function () { return tg.file("Hello, World!"); }
	',
}

let cli_output = tg build --network=true $cli_path | complete
failure $cli_output
snapshot --normalize-ids --redact $cli_path $cli_output.stderr '
	error an error occurred
	-> a build must be cacheable

'
