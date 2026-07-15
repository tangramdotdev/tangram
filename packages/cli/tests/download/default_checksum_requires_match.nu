use ../../test.nu *

# Downloading a URL without a checksum fails, because the default checksum matches nothing and the caller must opt in with a wildcard.

skip_if_offline

let server = spawn

let output = tg download "http://www.example.com" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> checksum mismatch
	   actual = sha512:356a71a6fd7862385ab9884781f11be233c4ee6b9d380b4dffd428e75d2cc6d4d49139080f039f5a4792d20e558e1931b594b94a8efe4d2c2d0f6d147ee6f134
	   expected = sha512:none

'
