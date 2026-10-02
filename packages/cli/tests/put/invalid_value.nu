use ../lib/test.nu *

# Putting input that does not parse as a value fails with a parse error.

let local = server spawn

let output = tg put --no-tokens 'tg.bogus(((' | complete
failure $output
snapshot --normalize ($output.stderr | str replace --regex '(?m)[ \t]+$' '') '
	error an error occurred
	-> failed to parse the value
	->

'
