use {std::net::Ipv4Addr, tangram_client::prelude::*};

const FILTER_TABLE: &[&str] = &[];
const NAT_TABLE: &[&str] = &["-t", "nat"];
pub(crate) const TAP_INTERFACE_NAME_PREFIX: &str = "tg-";

#[derive(Debug)]
struct IptablesRule {
	table: &'static [&'static str],
	rule: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct IptablesRuleGuard {
	rule: IptablesRule,
}

impl IptablesRule {
	fn new<const N: usize>(table: &'static [&'static str], rule: [&str; N]) -> Self {
		Self {
			table,
			rule: rule.into_iter().map(str::to_owned).collect(),
		}
	}

	fn with_rule(table: &'static [&'static str], rule: Vec<String>) -> Self {
		Self { table, rule }
	}
}

impl IptablesRuleGuard {
	fn new(rule: IptablesRule) -> tg::Result<Self> {
		let rule_args = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		insert_iptables_rule(rule.table, &rule_args)?;
		Ok(Self { rule })
	}
}

/// Best-effort removal of host-wide iptables rules that prior runs of the server
/// may have left behind. If the `iptables` binary reports a permission error,
/// log a warning and return Ok rather than failing; the server can still start
/// without networking, and the operator may not have `CAP_NET_ADMIN`.
pub(crate) fn cleanup_persistent_rules(bridge: Option<&str>) -> tg::Result<()> {
	for rule in cleanup_persistent_rule_commands(bridge) {
		let rule_args = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		delete_iptables_rule(rule.table, &rule_args)?;
	}
	if let Some(bridge) = bridge {
		delete_bridge_masquerade_rules(bridge)?;
	}
	Ok(())
}

fn cleanup_persistent_rule_commands(bridge: Option<&str>) -> Vec<IptablesRule> {
	let tap_prefix = format!("{TAP_INTERFACE_NAME_PREFIX}+");
	let mut rules = Vec::new();
	if bridge.is_none() {
		rules.push(IptablesRule::new(
			NAT_TABLE,
			[
				"POSTROUTING",
				"-s",
				"172.16.0.0/12",
				"!",
				"-o",
				tap_prefix.as_str(),
				"-j",
				"MASQUERADE",
			],
		));
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			["FORWARD", "-i", tap_prefix.as_str(), "-j", "ACCEPT"],
		));
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			[
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
		));
	}
	if let Some(bridge) = bridge {
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			["FORWARD", "-i", bridge, "-j", "ACCEPT"],
		));
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			[
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
		));
	}
	rules
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

pub(crate) fn add_port_forwarding_rules(
	out_interface: &str,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<IptablesRuleGuard>> {
	let mut guards = Vec::new();
	for port in ports {
		let host = port
			.host
			.ok_or_else(|| tg::error!("expected a resolved host port"))?;
		if !host.is_single() || !port.guest.is_single() {
			return Err(tg::error!("expected resolved port mappings"));
		}
		let protocol = match port.protocol {
			tg::sandbox::PortProtocol::Tcp => "tcp",
			tg::sandbox::PortProtocol::Udp => "udp",
		};
		for chain in ["PREROUTING", "OUTPUT"] {
			guards.push(IptablesRuleGuard::new(port_nat_rule(
				chain,
				protocol,
				port.host_ip,
				host.start,
				guest_ip,
				port.guest.start,
			))?);
		}
		guards.push(IptablesRuleGuard::new(port_forward_rule(
			out_interface,
			protocol,
			guest_ip,
			port.guest.start,
		))?);
	}
	Ok(guards)
}

fn port_nat_rule(
	chain: &str,
	protocol: &str,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	guest_ip: Ipv4Addr,
	guest_port: u16,
) -> IptablesRule {
	let mut rule = vec![
		chain.to_owned(),
		"-p".to_owned(),
		protocol.to_owned(),
		"-m".to_owned(),
		protocol.to_owned(),
		"--dport".to_owned(),
		host_port.to_string(),
	];
	if let Some(host_ip) = host_ip.filter(|host_ip| !host_ip.is_unspecified()) {
		rule.push("-d".to_owned());
		rule.push(host_ip.to_string());
	} else {
		rule.push("-m".to_owned());
		rule.push("addrtype".to_owned());
		rule.push("--dst-type".to_owned());
		rule.push("LOCAL".to_owned());
	}
	rule.extend([
		"-j".to_owned(),
		"DNAT".to_owned(),
		"--to-destination".to_owned(),
		format!("{guest_ip}:{guest_port}"),
	]);
	IptablesRule::with_rule(NAT_TABLE, rule)
}

fn port_forward_rule(
	out_interface: &str,
	protocol: &str,
	guest_ip: Ipv4Addr,
	guest_port: u16,
) -> IptablesRule {
	IptablesRule::with_rule(
		FILTER_TABLE,
		vec![
			"FORWARD".to_owned(),
			"-o".to_owned(),
			out_interface.to_owned(),
			"-p".to_owned(),
			protocol.to_owned(),
			"-m".to_owned(),
			protocol.to_owned(),
			"-d".to_owned(),
			guest_ip.to_string(),
			"--dport".to_owned(),
			guest_port.to_string(),
			"-j".to_owned(),
			"ACCEPT".to_owned(),
		],
	)
}

fn insert_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
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

fn delete_iptables_rule_once(table: &[&str], rule: &[&str]) -> tg::Result<()> {
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
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if is_iptables_permission_error(&stderr) {
		tracing::warn!(rule = %rule.join(" "), %stderr, "iptables cleanup denied");
	}
	Ok(())
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

impl Drop for IptablesRuleGuard {
	fn drop(&mut self) {
		let rule_args = self
			.rule
			.rule
			.iter()
			.map(String::as_str)
			.collect::<Vec<_>>();
		if let Err(error) = delete_iptables_rule_once(self.rule.table, &rule_args) {
			tracing::error!(%error, "failed to clean up the sandbox port forwarding rule");
		}
	}
}
