use tangram_client::prelude::*;

pub(crate) const TAP_INTERFACE_NAME_PREFIX: &str = "tg-";

/// Best-effort removal of host-wide iptables rules that prior runs of the server
/// may have left behind. If the `iptables` binary reports a permission error,
/// log a warning and return Ok rather than failing; the server can still start
/// without networking, and the operator may not have `CAP_NET_ADMIN`.
pub(crate) fn cleanup_persistent_rules(bridge: Option<&str>) -> tg::Result<()> {
	let tap_prefix = format!("{TAP_INTERFACE_NAME_PREFIX}+");
	delete_iptables_rule(
		&["-t", "nat"],
		&[
			"POSTROUTING",
			"-s",
			"172.16.0.0/12",
			"!",
			"-o",
			tap_prefix.as_str(),
			"-j",
			"MASQUERADE",
		],
	)?;
	delete_iptables_rule(&[], &["FORWARD", "-i", tap_prefix.as_str(), "-j", "ACCEPT"])?;
	delete_iptables_rule(
		&[],
		&[
			"FORWARD",
			"-o",
			tap_prefix.as_str(),
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)?;
	if let Some(bridge) = bridge {
		delete_iptables_rule(&[], &["FORWARD", "-i", bridge, "-j", "ACCEPT"])?;
		delete_iptables_rule(
			&[],
			&[
				"FORWARD",
				"-o",
				bridge,
				"-m",
				"conntrack",
				"--ctstate",
				"ESTABLISHED,RELATED",
				"-j",
				"ACCEPT",
			],
		)?;
		delete_bridge_masquerade_rules(bridge)?;
	}
	Ok(())
}

pub(crate) fn enable_ipv4_forwarding() -> tg::Result<()> {
	std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")
		.map_err(|error| tg::error!(!error, "failed to enable ipv4 forwarding"))
}

pub(crate) fn get_or_set_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	let mut check: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
	check.extend_from_slice(table);
	check.push("-C");
	check.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&check)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}
	let mut insert: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
	insert.extend_from_slice(table);
	insert.push("-I");
	insert.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&insert)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let rule = rule.join(" ");
		return Err(tg::error!(%stderr, %rule, "failed to install iptables rule"));
	}
	Ok(())
}

fn delete_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	loop {
		let mut args: Vec<&str> = Vec::with_capacity(table.len() + 1 + rule.len());
		args.extend_from_slice(table);
		args.push("-D");
		args.extend_from_slice(rule);
		let output = match std::process::Command::new("iptables")
			.args(&args)
			.stderr(std::process::Stdio::piped())
			.output()
		{
			Ok(output) => output,
			Err(error) => {
				if error.kind() == std::io::ErrorKind::NotFound {
					tracing::warn!("iptables not found; skipping rule cleanup");
					return Ok(());
				}
				return Err(tg::error!(!error, "failed to spawn iptables"));
			},
		};
		if output.status.success() {
			continue;
		}
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(rule = %rule.join(" "), %stderr, "iptables cleanup denied");
			return Ok(());
		}
		return Ok(());
	}
}

fn delete_bridge_masquerade_rules(bridge: &str) -> tg::Result<()> {
	let output = match std::process::Command::new("iptables")
		.args(["-t", "nat", "-S", "POSTROUTING"])
		.stderr(std::process::Stdio::piped())
		.output()
	{
		Ok(output) => output,
		Err(error) => {
			if error.kind() == std::io::ErrorKind::NotFound {
				tracing::warn!("iptables not found; skipping rule cleanup");
				return Ok(());
			}
			return Err(tg::error!(!error, "failed to spawn iptables"));
		},
	};
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(%stderr, "iptables cleanup denied");
		}
		return Ok(());
	}
	let stdout = String::from_utf8_lossy(&output.stdout);
	let needle = format!("! -o {bridge} ");
	for line in stdout.lines() {
		let Some(rest) = line.strip_prefix("-A POSTROUTING ") else {
			continue;
		};
		if !rest.contains(&needle) || !rest.contains("MASQUERADE") {
			continue;
		}
		let mut args = vec!["-t", "nat", "-D", "POSTROUTING"];
		args.extend(rest.split_whitespace());
		let output = match std::process::Command::new("iptables")
			.args(&args)
			.stderr(std::process::Stdio::piped())
			.output()
		{
			Ok(output) => output,
			Err(error) => return Err(tg::error!(!error, "failed to spawn iptables")),
		};
		if output.status.success() {
			continue;
		}
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_iptables_permission_error(&stderr) {
			tracing::warn!(rule = %rest, %stderr, "iptables cleanup denied");
			return Ok(());
		}
	}
	Ok(())
}

fn is_iptables_permission_error(stderr: &str) -> bool {
	let stderr = stderr.to_ascii_lowercase();
	stderr.contains("permission denied")
		|| stderr.contains("operation not permitted")
		|| stderr.contains("you must be root")
}
