_tg_shell_eval() {
	local output
	output="$(command tangram "$@")" || return $?
	if [[ -n "$output" ]]; then
		eval "$output"
	fi
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

if ! (( ${chpwd_functions[(I)_tg_shell_update]} )); then
	chpwd_functions+=(_tg_shell_update)
fi
if ! (( ${precmd_functions[(I)_tg_shell_update]} )); then
	precmd_functions+=(_tg_shell_update)
fi

_tg_shell_update
