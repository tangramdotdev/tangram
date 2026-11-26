%global _rpmformat 4
%global _binary_payload w6.gzdio
# Preserve the prebuilt payload and allow packaging it on a different architecture.
%global __spec_install_post %{nil}
%global _build_id_links none
%global debug_package %{nil}
%global source_date_epoch_from_changelog 0

Name: tangram
Version: %{tangram_version}
Release: 1
Summary: Tangram build system and package manager
License: MIT
URL: https://tangram.dev
Source0: tangram.tar.gz
Source1: config.json
Source2: tangram.service
Source3: LICENSE
Requires: ca-certificates, fuse3, iptables, nftables, passt, squashfs-tools, systemd
Requires(pre): coreutils, grep, shadow-utils
Requires(postun): sed

%description
Includes the Tangram CLI and a systemd-managed server.

%install
install -d "%{buildroot}%{_bindir}"
tar -xzf "%{SOURCE0}" -C "%{buildroot}%{_bindir}" tangram tg tgx
install -Dm644 "%{SOURCE1}" "%{buildroot}%{_sysconfdir}/tangram/config.json"
install -Dm644 "%{SOURCE2}" "%{buildroot}%{_prefix}/lib/systemd/system/tangram.service"
install -Dm644 "%{SOURCE3}" "%{buildroot}%{_datadir}/licenses/%{name}/LICENSE"

%pre
getent group tangram >/dev/null || groupadd --system tangram

# Register a subordinate id range for the server's sandboxes after the ranges that are already registered.
for file in /etc/subuid /etc/subgid; do
	touch "$file"
	if ! grep -q '^tangram:' "$file"; then
		start=100000
		while IFS=: read -r name first count || [ -n "$name" ]; do
			case "$first" in '' | *[!0-9]*) continue ;; esac
			case "$count" in '' | *[!0-9]*) continue ;; esac
			if [ $((first + count)) -gt "$start" ]; then
				start=$((first + count))
			fi
		done < "$file"
		if [ $((start + 65536)) -le 4294967295 ]; then
			if [ -s "$file" ] && [ -n "$(tail -c 1 "$file")" ]; then
				echo >> "$file"
			fi
			echo "tangram:$start:65536" >> "$file"
		fi
	fi
done

%post
if [ "$1" -eq 1 ]; then
	systemctl --no-reload enable tangram.service || :
fi
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || :
	if [ "$1" -eq 1 ]; then
		systemctl start tangram.service || :
	else
		systemctl try-restart tangram.service || :
	fi
fi

%preun
if [ "$1" -eq 0 ]; then
	if [ -d /run/systemd/system ]; then
		systemctl stop tangram.service || :
	fi
	systemctl --no-reload disable tangram.service || :
fi

%postun
if [ "$1" -eq 0 ] && [ -d /run/systemd/system ]; then
	systemctl daemon-reload || :
fi
if [ "$1" -eq 0 ]; then
	# Remove the subordinate id range of the server's sandboxes.
	for file in /etc/subuid /etc/subgid; do
		if [ -f "$file" ]; then
			sed -i '/^tangram:/d' "$file"
		fi
	done
fi

%files
%{_bindir}/tangram
%{_bindir}/tg
%{_bindir}/tgx
%dir %{_sysconfdir}/tangram
%config(noreplace) %{_sysconfdir}/tangram/config.json
%{_prefix}/lib/systemd/system/tangram.service
%license %{_datadir}/licenses/%{name}/LICENSE
