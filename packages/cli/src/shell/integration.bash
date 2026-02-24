_tg_shell_eval() {
	local output_path status
	output_path="$(mktemp)" || return $?
	command tangram "$@" >"$output_path"
	status="$?"
	if [[ "$status" -ne 0 ]]; then
		rm -f "$output_path"
		return "$status"
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
			_tg_shell_eval shell activate bash "$@"
			;;
	esac
}

_tg_shell_deactivate() {
	case "${1-}" in
		bash|fish|nu|zsh)
			_tg_shell_eval shell deactivate "$1"
			;;
		*)
			_tg_shell_eval shell deactivate bash
			;;
	esac
}

_tg_shell_update() {
	_tg_shell_eval shell directory update bash
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

if [[ -n "${PROMPT_COMMAND:-}" ]]; then
	case ";$PROMPT_COMMAND;" in
		*";_tg_shell_update;"*)
			;;
		*)
			PROMPT_COMMAND="_tg_shell_update;$PROMPT_COMMAND"
			;;
	esac
else
	PROMPT_COMMAND="_tg_shell_update"
fi

_tg_shell_update
