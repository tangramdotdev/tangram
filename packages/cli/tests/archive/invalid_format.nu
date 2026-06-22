use ../../test.nu *

# Unsupported archive format and compression values are rejected by the command line parser.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let format_output = tg archive --format rar $dir | complete
failure $format_output
snapshot ($format_output.stderr | redact) r#'
	error: invalid value 'rar' for '--format <FORMAT>': Invalid `ArchiveFormat` string representation
	
	For more information, try '--help'.

'#

let compression_output = tg archive --format tar --compression lz4 $dir | complete
failure $compression_output
snapshot ($compression_output.stderr | redact) r#'
	error: invalid value 'lz4' for '--compression <COMPRESSION>': Invalid `CompressionFormat` string representation
	
	For more information, try '--help'.

'#
