use ../../../test.nu *

# tg.Checksum.algorithm throws when the checksum has neither a colon nor a dash separator.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			try {
				tg.Checksum.algorithm("noseparator");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"invalid checksum"'
