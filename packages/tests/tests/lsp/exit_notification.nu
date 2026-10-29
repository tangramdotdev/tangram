use ../../lib/test.nu *
use ../../lib/lsp.nu

# The language server exits after the exit notification even though its client keeps stdin open, as an editor does.

let local = server spawn
let messages = [
	(lsp initialize 1)
	(lsp initialized)
	(lsp request 2 'shutdown' null)
	(lsp notification 'exit' null)
] | str join ''
let script = '
import subprocess, sys
process = subprocess.Popen(["tg", "lsp"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
process.stdin.write(sys.argv[1].encode())
process.stdin.flush()
try:
	print(process.wait(timeout=10))
except subprocess.TimeoutExpired:
	process.kill()
	process.wait()
	print("timeout")
'
let output = python3 -c $script $messages | complete
success $output
assert equal ($output.stdout | str trim) '0' 'the language server should exit with code 0 without waiting for stdin to close'
