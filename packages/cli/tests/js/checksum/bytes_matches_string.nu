use ../../../test.nu *

# tg.checksum produces the same digest for a string and for its UTF-8 byte array.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let fromString = await tg.checksum("hello", "sha256");
			let fromBytes = await tg.checksum(tg.encoding.utf8.encode("hello"), "sha256");
			return fromString === fromBytes;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
