use ../../test.nu *

# An unsupported algorithm is rejected by the command line parser.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg checksum --algorithm crc32 $blob | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'crc32' for '--algorithm <ALGORITHM>': Invalid `Algorithm` string representation
	
	For more information, try '--help'.

'#
