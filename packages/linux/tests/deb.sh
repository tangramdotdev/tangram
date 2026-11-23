#!/bin/sh
set -eu

# Run inside a privileged Debian container booted with systemd.
initial=$1
upgrade=$2
socket=/var/lib/tangram/socket
export DEBIAN_FRONTEND=noninteractive

wait_for_server() {
	curl --fail --silent --show-error --retry 12 --retry-delay 1 --retry-all-errors \
		--unix-socket "$socket" http://localhost/health >/dev/null
}

apt-get install -y "$initial"
systemctl --quiet is-enabled tangram.service
wait_for_server
findmnt --mountpoint /var/lib/tangram/store --noheadings --output FSTYPE | grep '^fuse'

useradd --groups tangram tg-member
useradd tg-outsider
runuser -u tg-member -- tg health
if runuser -u tg-outsider -- tg health; then
	echo "a user outside the tangram group connected to the server" >&2
	exit 1
fi
if runuser -u tg-member -- ls /var/lib/tangram >/dev/null 2>&1; then
	echo "a member of the tangram group listed the server directory" >&2
	exit 1
fi

artifact=$(tg put --no-tokens 'tg.file("shared artifact")')
checkout=$(runuser -u tg-member -- tg checkout "$artifact")
test "$(runuser -u tg-member -- cat "$checkout")" = 'shared artifact'
test "$(runuser -u tg-member -- cat "/var/lib/tangram/store/${artifact%%\?*}")" = 'shared artifact'

pid=$(systemctl show --property=MainPID --value tangram.service)
apt-get install -y "$upgrade"
wait_for_server
test "$(systemctl show --property=MainPID --value tangram.service)" != "$pid"
test "$(runuser -u tg-member -- cat "$checkout")" = 'shared artifact'

systemctl disable --now tangram.service
apt-get install -y --allow-downgrades "$initial"
if systemctl --quiet is-active tangram.service || systemctl --quiet is-enabled tangram.service; then
	echo "the package reactivated a stopped and disabled service" >&2
	exit 1
fi

systemctl start tangram.service
wait_for_server
apt-get purge -y tangram
test ! -S "$socket"
test -d /var/lib/tangram
