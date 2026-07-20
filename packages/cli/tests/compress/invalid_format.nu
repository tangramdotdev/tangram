use ../../test.nu *

# Compressing with an unsupported format is rejected by the command line parser.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg compress --format lz4 $blob | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'lz4' for '--format <FORMAT>': Invalid `CompressionFormat` string representation
	
	For more information, try '--help'.

'#
