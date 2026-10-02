use ../lib/test.nu *

# An unsupported algorithm is rejected by the command line parser.

let local = server spawn

let blob = "hello, world!\n" | tg write --no-tokens | referent node

let output = tg checksum --algorithm crc32 $blob | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'crc32' for '--algorithm <ALGORITHM>': Invalid `Algorithm` string representation

	For more information, try '--help'.

'#
