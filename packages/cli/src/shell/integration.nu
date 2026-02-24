def --env __tg_shell_apply [output] {
	for line in ($output | lines) {
		let line = ($line | str trim)
		if ($line | is-empty) {
			continue
		}
		if ($line | str starts-with "load-env ") {
			let value = ($line | str replace -r '^load-env\s+' '' | from nuon)
			load-env $value
			continue
		}
		if ($line | str starts-with "hide-env ") {
			let name = ($line | str replace -r '^hide-env\s+' '')
			hide-env $name
			continue
		}
		print --stderr $"unsupported command: ($line)"
	}
}

def --env __tg_shell_eval [...argv] {
	let result = (^tangram ...$argv | complete)
	if ($result.stderr | str length) > 0 {
		print --stderr $result.stderr
	}
	if $result.exit_code != 0 {
		return
	}
	if ($result.stdout | str length) == 0 {
		return
	}
	__tg_shell_apply $result.stdout
}

def --env __tg_shell_dispatch [...argv] {
	if (($argv | length) >= 2 and $argv.0 == "shell" and $argv.1 == "activate") {
		let trailing = ($argv | skip 2)
		if (($trailing | length) > 0 and (($trailing | first) in ["bash", "fish", "nu", "zsh"])) {
			__tg_shell_eval shell activate ...$trailing
		} else {
			__tg_shell_eval shell activate nu ...$trailing
		}
		return
	}
	if (($argv | length) >= 2 and $argv.0 == "shell" and $argv.1 == "deactivate") {
		let trailing = ($argv | skip 2)
		if (($trailing | length) > 0 and (($trailing | first) in ["bash", "fish", "nu", "zsh"])) {
			__tg_shell_eval shell deactivate ($trailing | first)
		} else {
			__tg_shell_eval shell deactivate nu
		}
		return
	}
	^tangram ...$argv
}

def --env --wrapped tg [...argv] {
	__tg_shell_dispatch ...$argv
}

def --env --wrapped tangram [...argv] {
	__tg_shell_dispatch ...$argv
}

def --env __tg_shell_update [] {
	__tg_shell_eval shell directory update nu
}

def --env __tg_shell_install_pwd_hook [] {
	if ($env | get -o TANGRAM_SHELL_NU_HOOK_INSTALLED | default false) {
		return
	}
	let existing = ($env.config | get -o hooks env_change PWD | default [])
	let existing = if (($existing | describe) | str starts-with "record") {
		[$existing]
	} else {
		$existing
	}
	let existing = (
		$existing
			| each {|hook|
				if (($hook | describe) | str starts-with "closure") {
					{ code: $hook }
				} else if (($hook | describe) | str starts-with "record") {
					if (($hook | columns | any {|column| $column == "code"})) {
						$hook
					} else {
						null
					}
				} else {
					null
				}
		}
		| where {|hook| $hook != null }
	)
	let hooks = $existing
	let hook = { code: {|before, after| __tg_shell_update } }
	$env.config = (
		$env.config
		| upsert hooks.env_change.PWD ($hooks | append $hook)
	)
	load-env { TANGRAM_SHELL_NU_HOOK_INSTALLED: true }
}

__tg_shell_install_pwd_hook
__tg_shell_update
