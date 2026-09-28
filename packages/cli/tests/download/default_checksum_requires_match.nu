use ../lib/test.nu *

# Downloading a URL without a checksum fails, because the default checksum matches nothing and the caller must opt in with a wildcard.

skip_if_offline

let server = server spawn

let output = tg download "http://www.example.com" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> checksum mismatch
	   actual = sha512:e2f3e5ba74cf7c6074d4080fdb3af0d65b75fe1628734c0cb6a9f8e4bf4fe682b40522c943285fcb051461457730182dd5452f24de701588df2b876be5f010ee
	   expected = sha512:none

'
