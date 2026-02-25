_tg_shell_eval() {
	local output_path exit_code
	output_path="$(mktemp)" || return $?
	command tangram "$@" >"$output_path"
	exit_code="$?"
	if [[ "$exit_code" -ne 0 ]]; then
		rm -f "$output_path"
		return "$exit_code"
	fi
	if [[ -s "$output_path" ]]; then
		source "$output_path"
	fi
	rm -f "$output_path"
}

_tg_shell_activate() {
	case "${1-}" in
		bash|fish|nu|zsh)
			_tg_shell_eval shell activate "$@"
			;;
		*)
			_tg_shell_eval shell activate zsh "$@"
			;;
	esac
}

_tg_shell_deactivate() {
	case "${1-}" in
		bash|fish|nu|zsh)
			_tg_shell_eval shell deactivate "$1"
			;;
		*)
			_tg_shell_eval shell deactivate zsh
			;;
	esac
}

_tg_shell_update() {
	_tg_shell_eval shell directory update zsh
}

_tg_shell_dispatch() {
	if [[ "${1-}" == "shell" && "${2-}" == "activate" ]]; then
		shift 2
		_tg_shell_activate "$@"
		return $?
	fi
	if [[ "${1-}" == "shell" && "${2-}" == "deactivate" ]]; then
		_tg_shell_deactivate
		return $?
	fi
	command tangram "$@"
}

tg() {
	_tg_shell_dispatch "$@"
}

tangram() {
	_tg_shell_dispatch "$@"
}

typeset -ga chpwd_functions
typeset -ga precmd_functions

autoload -Uz add-zsh-hook
add-zsh-hook -D chpwd _tg_shell_update >/dev/null 2>&1
add-zsh-hook -D precmd _tg_shell_update >/dev/null 2>&1
add-zsh-hook chpwd _tg_shell_update
add-zsh-hook precmd _tg_shell_update

_tg_shell_update
