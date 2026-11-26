#!/bin/sh
set -u

# Run the server in the foreground when the command is a server command.
case "${1-}" in
	serve | server)
		exec tangram "$@"
		;;
esac

# Forward a signal to the command.
child=
forward() {
	if [ -n "$child" ]; then
		kill -s "$1" "$child" 2>/dev/null
	fi
}
trap 'forward HUP' HUP
trap 'forward INT' INT
trap 'forward QUIT' QUIT
trap 'forward TERM' TERM

# Run the command in the background with the shell's standard input and with the default actions for SIGINT and SIGQUIT, which the shell otherwise ignores in background commands. The command starts a server if it needs one, because the image sets TANGRAM_START.
exec 3<&0
env --default-signal=INT,QUIT tangram "$@" <&3 3<&- &
child=$!
exec 3<&-

# Wait for the command to exit, and wait again when a trapped signal interrupts the wait before the command has been reaped.
while :; do
	wait "$child"
	status=$?
	if ! kill -0 "$child" 2>/dev/null; then
		break
	fi
done

# Stop the server that the command started, if it started one, and exit with the status of the command.
tangram server stop
exit "$status"
