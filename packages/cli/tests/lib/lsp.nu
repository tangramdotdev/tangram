export def uri [path: string] {
	$"file://($path | path expand)"
}

export def path_for_uri [uri: string] {
	$uri | str replace --regex '^file://' '' | url decode
}

export def request [id: int, method: string, params?: any] {
	let message = if $params == null {
		{ jsonrpc: "2.0", id: $id, method: $method }
	} else {
		{ jsonrpc: "2.0", id: $id, method: $method, params: $params }
	}
	frame $message
}

export def notification [method: string, params?: any] {
	let message = if $params == null {
		{ jsonrpc: "2.0", method: $method }
	} else {
		{ jsonrpc: "2.0", method: $method, params: $params }
	}
	frame $message
}

export def initialize [id: int] {
	request $id "initialize" {
		processId: null,
		capabilities: {},
		rootUri: null,
	}
}

export def initialized [] {
	notification "initialized" {}
}

export def did_open [uri: string, text: string, version: int = 1] {
	notification "textDocument/didOpen" {
		textDocument: {
			uri: $uri,
			languageId: "tangram-typescript",
			version: $version,
			text: $text,
		},
	}
}

export def definition [id: int, uri: string, line: int, character: int] {
	request $id "textDocument/definition" {
		textDocument: { uri: $uri },
		position: { line: $line, character: $character },
	}
}

export def document_link [id: int, uri: string] {
	request $id "textDocument/documentLink" {
		textDocument: { uri: $uri },
	}
}

export def hover [id: int, uri: string, line: int, character: int] {
	request $id "textDocument/hover" {
		textDocument: { uri: $uri },
		position: { line: $line, character: $character },
	}
}

export def diagnostics [id: int, uri: string] {
	request $id "textDocument/diagnostic" {
		textDocument: { uri: $uri },
	}
}

export def rename [id: int, uri: string, line: int, character: int, new_name: string] {
	request $id "textDocument/rename" {
		textDocument: { uri: $uri },
		position: { line: $line, character: $character },
		newName: $new_name,
	}
}

export def run [messages: list<string>] {
	let shutdown_id = 999_998
	let input = (
		$messages
		| append (request $shutdown_id "shutdown" null)
		| append (notification "exit" null)
		| str join ""
	)
	let output = $input | tg lsp | complete
	if $output.exit_code != 0 {
		error make { msg: $"lsp exited with code ($output.exit_code): ($output.stderr)" }
	}
	parse_output $output.stdout
}

export def start [] {
	let directory = mktemp -d
	let input = $directory | path join "input"
	let output = $directory | path join "output"
	let stderr = $directory | path join "stderr"
	let pid = $directory | path join "pid"
	let exit_directory = (($env.TMPDIR? | default $directory) | path join "server_jobs")
	try { mkdir $exit_directory }
	mkfifo $input
	"" | save $output
	"" | save $stderr
	let job = job spawn -d lsp {
		let lsp_job_id = job id
		let exit_path = $exit_directory | path join $'($lsp_job_id).exit'
		do -i {
			bash -c $"
				PARENT_PID=$PPID
				SELF_PID=$$
				printf '%s\n' \"$SELF_PID\" > \"($pid)\"
				\(
					while kill -0 $PARENT_PID 2>/dev/null; do
						sleep 1
					done
					kill -9 -$SELF_PID
				\) &
				export DYLD_LIBRARY_PATH=\"($env.DYLD_LIBRARY_PATH? | default '')\"
				export DYLD_FALLBACK_LIBRARY_PATH=\"($env.DYLD_FALLBACK_LIBRARY_PATH? | default '')\"
				exec 3<>\"($input)\"
				cat <&3 | tg lsp > \"($output)\" 2> \"($stderr)\"
			"
		}
		'' | save -f $exit_path
	}
	{
		directory: $directory,
		exit: ($exit_directory | path join $'($job).exit'),
		input: $input,
		output: $output,
		stderr: $stderr,
		pid: $pid,
		offset: 0,
		inbox: [],
		job: $job,
	}
}

export def send [session: record, message: string] {
	$message | bash -c 'cat > "$1"' _ $session.input
	$session
}

export def send_all [session: record, messages: list<string>] {
	$messages | str join "" | bash -c 'cat > "$1"' _ $session.input
	$session
}

export def read [session: record, --timeout: duration = 5sec] {
	let start = date now
	loop {
		let output = open $session.output
		let parsed = parse_next_output $output $session.offset
		if $parsed != null {
			return {
				session: ($session | update offset $parsed.offset),
				message: $parsed.message,
			}
		}
		if ((date now) - $start) > $timeout {
			let stderr = open $session.stderr
			error make { msg: $"timed out waiting for an lsp message: ($stderr)" }
		}
		sleep 20ms
	}
}

export def wait_response [session: record, id: int, --timeout: duration = 5sec] {
	mut session = $session
	mut messages = []
	let start = date now
	loop {
		let matches = $session.inbox | enumerate | where { |entry| $entry.item.id? == $id }
		if not ($matches | is-empty) {
			let matched = $matches | first
			let message = $matched.item
			$session = $session | update inbox (
				$session.inbox
				| enumerate
				| where { |entry| $entry.index != $matched.index }
				| each { |entry| $entry.item }
			)
			if $message.error? != null {
				error make { msg: $"lsp response ($id) failed: ($message.error | to json --raw)" }
			}
			return {
				session: $session,
				message: $message,
				messages: ($messages | append $message),
			}
		}
		let elapsed = (date now) - $start
		if $elapsed > $timeout {
			let stderr = open $session.stderr
			error make { msg: $"timed out waiting for lsp response ($id): ($stderr)" }
		}
		let read = read $session --timeout ($timeout - $elapsed)
		$session = $read.session
		let message = $read.message
		$messages = $messages | append $message
		if $message.id? == $id {
			if $message.error? != null {
				error make { msg: $"lsp response ($id) failed: ($message.error | to json --raw)" }
			}
			return {
				session: $session,
				message: $message,
				messages: $messages,
			}
		}
		$session = $session | update inbox ($session.inbox | append $message)
	}
}

export def wait_result [session: record, id: int, --timeout: duration = 5sec] {
	let output = wait_response $session $id --timeout $timeout
	{
		session: $output.session,
		result: $output.message.result,
		messages: $output.messages,
	}
}

export def stop [session: record] {
	try {
		[
			(request 999_998 "shutdown" null)
			(notification "exit" null)
		] | str join "" | timeout 2 bash -c 'cat > "$1"' _ $session.input | ignore
	}
	let exited = if ($session.exit | path exists) {
		true
	} else {
		let output = open /dev/null | timeout 5sec bash -c 'while [ ! -e "$1" ]; do sleep 0.05; done' _ $session.exit | complete
		$output.exit_code == 0 or ($session.exit | path exists)
	}
	if not $exited {
		kill_session_processes $session "TERM"
		let output = open /dev/null | timeout 1sec bash -c 'while [ ! -e "$1" ]; do sleep 0.05; done' _ $session.exit | complete
		let exited = $output.exit_code == 0 or ($session.exit | path exists)
		if not $exited {
			kill_session_processes $session "KILL"
		}
		try { job kill $session.job }
	}
	try { rm -rf $session.directory }
}

export def response [messages: list<any>, id: int] {
	let responses = $messages | where id? == $id
	if ($responses | is-empty) {
		error make { msg: $"missing lsp response ($id)" }
	}
	let message = $responses | first
	if $message.error? != null {
		error make { msg: $"lsp response ($id) failed: ($message.error | to json --raw)" }
	}
	$message
}

export def result [messages: list<any>, id: int] {
	response $messages $id | get result
}

def frame [message: record] {
	let content = $message | to json --raw
	let length = $content | encode utf8 | bytes length
	$"Content-Length: ($length)\r\n\r\n($content)"
}

def parse_next_output [output: string, offset: int] {
	let data = $output | encode utf8
	let separator = "\r\n\r\n" | encode utf8
	let total = $data | bytes length
	if $offset >= $total {
		return null
	}
	let relative_header_end = $data | bytes at $offset.. | bytes index-of $separator
	if $relative_header_end < 0 {
		return null
	}
	let header_end = $offset + $relative_header_end
	let header = $data | bytes at $offset..<$header_end | decode utf-8
	let length_line = $header | split row "\r\n" | where { |line| $line | str starts-with "Content-Length: " } | first
	let content_length = $length_line | str replace "Content-Length: " "" | into int
	let body_start = $header_end + 4
	let body_end = $body_start + $content_length
	if $body_end > $total {
		return null
	}
	let body = $data | bytes at $body_start..<$body_end | decode utf-8
	{
		offset: $body_end,
		message: ($body | from json),
	}
}

def parse_output [output: string] {
	let data = $output | encode utf8
	let separator = "\r\n\r\n" | encode utf8
	mut messages = []
	mut offset = 0
	let total = $data | bytes length
	loop {
		if $offset >= $total {
			break
		}
		let relative_header_end = $data | bytes at $offset.. | bytes index-of $separator
		if $relative_header_end < 0 {
			let rest = $data | bytes at $offset.. | decode utf-8 | str trim
			if $rest == "" {
				break
			}
			error make { msg: $"invalid lsp response frame: ($rest)" }
		}
		let header_end = $offset + $relative_header_end
		let header = $data | bytes at $offset..<$header_end | decode utf-8
		let length_line = $header | split row "\r\n" | where { |line| $line | str starts-with "Content-Length: " } | first
		let content_length = $length_line | str replace "Content-Length: " "" | into int
		let body_start = $header_end + 4
		let body_end = $body_start + $content_length
		if $body_end > $total {
			error make { msg: "incomplete lsp response frame" }
		}
		let body = $data | bytes at $body_start..<$body_end | decode utf-8
		$messages = $messages | append ($body | from json)
		$offset = $body_end
	}
	$messages
}

def kill_session_processes [session: record, signal: string] {
	mut pids = []
	if ($session.pid | path exists) {
		let pid = open $session.pid | str trim
		if $pid != "" {
			$pids = $pids | append $pid
		}
	}
	let jobs = job list | where { |job| $job.id == $session.job }
	for job in $jobs {
		$pids = $pids | append ($job.pids? | default [])
	}
	for pid in ($pids | uniq) {
		try {
			^bash -c 'kill -"$2" -- -"$1" 2>/dev/null || true; kill -"$2" "$1" 2>/dev/null || true' _ $pid $signal
		}
	}
}
