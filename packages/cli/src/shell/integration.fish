function __tg_shell_eval
	set -l output (command tangram $argv)
	set -l status_code $status
	if test $status_code -ne 0
		return $status_code
	end
	if test (count $output) -gt 0
		printf "%s\n" $output | source
	end
end

function __tg_shell_activate
	if test (count $argv) -ge 1
		switch $argv[1]
			case bash fish nu zsh
				__tg_shell_eval shell activate $argv
				return $status
		end
	end
	__tg_shell_eval shell activate fish $argv
end

function __tg_shell_deactivate
	if test (count $argv) -ge 1
		switch $argv[1]
			case bash fish nu zsh
				__tg_shell_eval shell deactivate $argv[1]
				return $status
		end
	end
	__tg_shell_eval shell deactivate fish
end

if functions -q __tg_shell_update
	functions --erase __tg_shell_update
end

function __tg_shell_update --on-variable PWD
	__tg_shell_eval shell directory update fish
end

function __tg_shell_dispatch
	if test (count $argv) -ge 2
		if test "$argv[1]" = "shell"; and test "$argv[2]" = "activate"
			__tg_shell_activate $argv[3..-1]
			return $status
		end
		if test "$argv[1]" = "shell"; and test "$argv[2]" = "deactivate"
			__tg_shell_deactivate
			return $status
		end
	end
	command tangram $argv
end

function tg
	__tg_shell_dispatch $argv
end

function tangram
	__tg_shell_dispatch $argv
end

__tg_shell_update
