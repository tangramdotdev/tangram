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

def __tg_shell_output_path [] {
	let directory = ($env | get -o TMPDIR | default $nu.temp-dir)
	let name = $"tg-shell-((random uuid))"
	$directory | path join $name
}

def --env __tg_shell_eval [...argv] {
	let output_path = (__tg_shell_output_path)
	'' | save --force --raw $output_path
	^tangram ...$argv o> $output_path
	if $env.LAST_EXIT_CODE != 0 {
		rm --force $output_path
		return
	}
	let output = (open --raw $output_path)
	rm --force $output_path
	if ($output | str length) == 0 {
		return
	}
	__tg_shell_apply $output
}

def __tg_shell_expand_reference [reference] {
	let parsed = ($reference | parse -r '^(?<path>[^#]+)(?:#(?<export>.*))?$')
	if ($parsed | is-empty) {
		return $reference
	}
	let parsed = ($parsed | first)
	if not ($parsed.path | str starts-with "~") {
		return $reference
	}
	let path = ($parsed.path | path expand)
	let export = ($parsed | get -o export)
	if $export == null {
		return $path
	}
	$"($path)#($export)"
}

def --env "tg shell activate" [reference, --base] {
	let reference = (__tg_shell_expand_reference $reference)
	if $base {
		__tg_shell_eval shell activate nu '--base' $reference
		return
	}
	__tg_shell_eval shell activate nu $reference
}

def --env "tangram shell activate" [reference, --base] {
	let reference = (__tg_shell_expand_reference $reference)
	if $base {
		__tg_shell_eval shell activate nu '--base' $reference
		return
	}
	__tg_shell_eval shell activate nu $reference
}

def --env "tg shell deactivate" [] {
	__tg_shell_eval shell deactivate nu
}

def --env "tangram shell deactivate" [] {
	__tg_shell_eval shell deactivate nu
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
