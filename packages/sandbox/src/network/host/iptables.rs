use {std::net::Ipv4Addr, tangram_client::prelude::*};

const FILTER_TABLE: &[&str] = &[];
const IPTABLES_WAIT: &str = "-w";
const NAT_TABLE: &[&str] = &["-t", "nat"];
const TANGRAM_DNAT_CHAIN: &str = "TANGRAM-DNAT";
const TANGRAM_BRIDGE_CHAIN: &str = "TANGRAM-BRIDGE";
const TANGRAM_FORWARD_CHAIN: &str = "TANGRAM-FWD";
const TANGRAM_INPUT_CHAIN: &str = "TANGRAM-INPUT";
const TANGRAM_SNAT_CHAIN: &str = "TANGRAM-SNAT";
const TANGRAM_RULE_COMMENT_PREFIX: &str = "tangram:sandbox_index=";

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

	fn chain(&self) -> Option<&str> {
		self.rule.first().map(String::as_str)
	}

	fn comment(&self) -> Option<&str> {
		self.rule
			.windows(2)
			.find_map(|window| (window[0] == "--comment").then(|| window[1].as_str()))
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

pub(crate) fn setup_tap_networking() -> tg::Result<()> {
	if let Err(error) = cleanup_persistent_rules(None) {
		tracing::warn!(%error, "failed to clean up persistent sandbox rules");
	}
	setup_port_forwarding()?;
	let prefix = format!("{}+", super::TAP_INTERFACE_NAME_PREFIX);

	// This rule makes guest VM traffic appear to come from the host IP, which prevents networks such as Wi-Fi access points from rejecting the packets.
	get_or_set_iptables_rule(
		NAT_TABLE,
		&[
			"POSTROUTING",
			"-s",
			"172.16.0.0/12",
			"!",
			"-o",
			prefix.as_str(),
			"-j",
			"MASQUERADE",
		],
	)?;

	// Forward the packets out of the guest.
	get_or_set_iptables_rule(
		FILTER_TABLE,
		&["FORWARD", "-i", prefix.as_str(), "-j", "ACCEPT"],
	)?;

	// Allow response packets to come back to the guest. Two rules instead of one to avoid outside connections from reaching the guest.
	get_or_set_iptables_rule(
		FILTER_TABLE,
		&[
			"FORWARD",
			"-o",
			prefix.as_str(),
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)?;

	Ok(())
}

pub(crate) fn setup_bridge_networking(
	bridge: &str,
	addr: Ipv4Addr,
	dns: &[Ipv4Addr],
) -> tg::Result<()> {
	if let Err(error) = cleanup_persistent_rules(Some(bridge)) {
		tracing::warn!(%error, "failed to clean up persistent sandbox rules");
	}
	setup_port_forwarding()?;
	ensure_iptables_chain(FILTER_TABLE, TANGRAM_BRIDGE_CHAIN)?;
	ensure_iptables_chain(FILTER_TABLE, TANGRAM_INPUT_CHAIN)?;
	flush_iptables_chain(FILTER_TABLE, TANGRAM_BRIDGE_CHAIN)?;
	flush_iptables_chain(FILTER_TABLE, TANGRAM_INPUT_CHAIN)?;
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	get_or_set_iptables_rule(
		NAT_TABLE,
		&[
			"POSTROUTING",
			"-s",
			cidr.as_str(),
			"!",
			"-o",
			bridge,
			"-j",
			"MASQUERADE",
		],
	)?;
	for rule in bridge_filter_rules(bridge, dns) {
		let rule = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		append_iptables_rule(FILTER_TABLE, &rule)?;
	}
	for rule in bridge_input_rules(bridge, dns) {
		let rule = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		append_iptables_rule(FILTER_TABLE, &rule)?;
	}
	replace_iptables_rule(
		FILTER_TABLE,
		&["FORWARD", "-i", bridge, "-j", TANGRAM_BRIDGE_CHAIN],
	)?;
	replace_iptables_rule(
		FILTER_TABLE,
		&["FORWARD", "-o", bridge, "-j", TANGRAM_BRIDGE_CHAIN],
	)?;
	replace_iptables_rule(
		FILTER_TABLE,
		&["INPUT", "-i", bridge, "-j", TANGRAM_INPUT_CHAIN],
	)?;
	// Published ports must be considered before the bridge chain rejects unsolicited inbound traffic.
	replace_iptables_rule(FILTER_TABLE, &["FORWARD", "-j", TANGRAM_FORWARD_CHAIN])?;
	Ok(())
}

fn bridge_filter_rules(bridge: &str, dns: &[Ipv4Addr]) -> Vec<IptablesRule> {
	let mut rules = vec![IptablesRule::new(
		FILTER_TABLE,
		[
			TANGRAM_BRIDGE_CHAIN,
			"-i",
			bridge,
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)];
	for addr in dns {
		for protocol in ["tcp", "udp"] {
			rules.push(IptablesRule::with_rule(
				FILTER_TABLE,
				vec![
					TANGRAM_BRIDGE_CHAIN.to_owned(),
					"-i".to_owned(),
					bridge.to_owned(),
					"-d".to_owned(),
					addr.to_string(),
					"-p".to_owned(),
					protocol.to_owned(),
					"--dport".to_owned(),
					"53".to_owned(),
					"-j".to_owned(),
					"ACCEPT".to_owned(),
				],
			));
		}
	}
	for cidr in private_networks() {
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			[TANGRAM_BRIDGE_CHAIN, "-i", bridge, "-d", cidr, "-j", "DROP"],
		));
	}
	rules.push(IptablesRule::new(
		FILTER_TABLE,
		[TANGRAM_BRIDGE_CHAIN, "-i", bridge, "-j", "ACCEPT"],
	));
	rules.push(IptablesRule::new(
		FILTER_TABLE,
		[
			TANGRAM_BRIDGE_CHAIN,
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
	rules.push(IptablesRule::new(
		FILTER_TABLE,
		[TANGRAM_BRIDGE_CHAIN, "-o", bridge, "-j", "DROP"],
	));
	rules
}

fn bridge_input_rules(bridge: &str, dns: &[Ipv4Addr]) -> Vec<IptablesRule> {
	let mut rules = vec![IptablesRule::new(
		FILTER_TABLE,
		[
			TANGRAM_INPUT_CHAIN,
			"-i",
			bridge,
			"-m",
			"conntrack",
			"--ctstate",
			"ESTABLISHED,RELATED",
			"-j",
			"ACCEPT",
		],
	)];
	for addr in dns {
		for protocol in ["tcp", "udp"] {
			rules.push(IptablesRule::with_rule(
				FILTER_TABLE,
				vec![
					TANGRAM_INPUT_CHAIN.to_owned(),
					"-i".to_owned(),
					bridge.to_owned(),
					"-d".to_owned(),
					addr.to_string(),
					"-p".to_owned(),
					protocol.to_owned(),
					"--dport".to_owned(),
					"53".to_owned(),
					"-j".to_owned(),
					"ACCEPT".to_owned(),
				],
			));
		}
	}
	rules.push(IptablesRule::new(
		FILTER_TABLE,
		[TANGRAM_INPUT_CHAIN, "-i", bridge, "-j", "DROP"],
	));
	rules
}

fn private_networks() -> &'static [&'static str] {
	&[
		"0.0.0.0/8",
		"10.0.0.0/8",
		"100.64.0.0/10",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"198.18.0.0/15",
		"224.0.0.0/4",
		"240.0.0.0/4",
	]
}

pub(crate) fn setup_port_forwarding() -> tg::Result<()> {
	static SETUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	SETUP.get_or_init(setup_port_forwarding_inner).clone()
}

fn cleanup_port_forwarding_rules() -> tg::Result<()> {
	static CLEANUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	CLEANUP
		.get_or_init(cleanup_port_forwarding_rules_inner)
		.clone()
}

fn setup_port_forwarding_inner() -> tg::Result<()> {
	ensure_iptables_chain(NAT_TABLE, TANGRAM_DNAT_CHAIN)?;
	ensure_iptables_chain(NAT_TABLE, TANGRAM_SNAT_CHAIN)?;
	ensure_iptables_chain(FILTER_TABLE, TANGRAM_FORWARD_CHAIN)?;

	get_or_set_iptables_rule(NAT_TABLE, &["PREROUTING", "-j", TANGRAM_DNAT_CHAIN])?;
	get_or_set_iptables_rule(NAT_TABLE, &["OUTPUT", "-j", TANGRAM_DNAT_CHAIN])?;
	get_or_set_iptables_rule(NAT_TABLE, &["POSTROUTING", "-j", TANGRAM_SNAT_CHAIN])?;
	get_or_set_iptables_rule(FILTER_TABLE, &["FORWARD", "-j", TANGRAM_FORWARD_CHAIN])?;

	Ok(())
}

fn cleanup_port_forwarding_rules_inner() -> tg::Result<()> {
	setup_port_forwarding()?;
	flush_iptables_chain(NAT_TABLE, TANGRAM_DNAT_CHAIN)?;
	flush_iptables_chain(NAT_TABLE, TANGRAM_SNAT_CHAIN)?;
	flush_iptables_chain(FILTER_TABLE, TANGRAM_FORWARD_CHAIN)?;
	Ok(())
}

fn cleanup_persistent_rule_commands(bridge: Option<&str>) -> Vec<IptablesRule> {
	let tap_prefix = format!("{}+", super::TAP_INTERFACE_NAME_PREFIX);
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

fn ensure_iptables_chain(table: &[&str], chain: &str) -> tg::Result<()> {
	let mut check = iptables_args(table, "-S");
	check.push(chain);
	let output = std::process::Command::new("iptables")
		.args(&check)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}

	let mut create = iptables_args(table, "-N");
	create.push(chain);
	let output = std::process::Command::new("iptables")
		.args(&create)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if is_iptables_chain_exists(&stderr) {
		return Ok(());
	}
	Err(tg::error!(%stderr, %chain, "failed to create the iptables chain"))
}

fn flush_iptables_chain(table: &[&str], chain: &str) -> tg::Result<()> {
	let mut args = iptables_args(table, "-F");
	args.push(chain);
	let output = std::process::Command::new("iptables")
		.args(&args)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	Err(tg::error!(%stderr, %chain, "failed to flush the iptables chain"))
}

pub(crate) fn get_or_set_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	let mut check = iptables_args(table, "-C");
	check.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&check)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if output.status.success() {
		return Ok(());
	}
	let mut insert = iptables_args(table, "-I");
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

fn append_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	let mut append = iptables_args(table, "-A");
	append.extend_from_slice(rule);
	let output = std::process::Command::new("iptables")
		.args(&append)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let rule = rule.join(" ");
		return Err(tg::error!(%stderr, %rule, "failed to append an iptables rule"));
	}
	Ok(())
}

fn replace_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	delete_iptables_rule(table, rule)?;
	insert_iptables_rule(table, rule)
}

pub(crate) fn add_port_forwarding_rules(
	index: u64,
	_identity: &std::path::Path,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<IptablesRuleGuard>> {
	setup_port_forwarding()?;
	cleanup_port_forwarding_rules()?;
	let mut guards = Vec::new();
	let comment = sandbox_rule_comment(index);
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
		guards.push(IptablesRuleGuard::new(port_nat_rule(
			protocol,
			port.host_ip,
			host.start,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
		guards.push(IptablesRuleGuard::new(port_forward_rule(
			out_interface,
			protocol,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
		guards.push(IptablesRuleGuard::new(port_snat_rule(
			out_interface,
			protocol,
			host_ip,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
	}
	Ok(guards)
}

fn port_nat_rule(
	protocol: &str,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> IptablesRule {
	let mut rule = vec![
		TANGRAM_DNAT_CHAIN.to_owned(),
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
	append_comment(&mut rule, comment);
	rule.extend([
		"-j".to_owned(),
		"DNAT".to_owned(),
		"--to-destination".to_owned(),
		format!("{guest_ip}:{guest_port}"),
	]);
	IptablesRule::with_rule(NAT_TABLE, rule)
}

fn port_snat_rule(
	out_interface: &str,
	protocol: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> IptablesRule {
	let mut rule = vec![
		TANGRAM_SNAT_CHAIN.to_owned(),
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
		"-m".to_owned(),
		"addrtype".to_owned(),
		"--src-type".to_owned(),
		"LOCAL".to_owned(),
	];
	append_comment(&mut rule, comment);
	rule.extend([
		"-j".to_owned(),
		"SNAT".to_owned(),
		"--to-source".to_owned(),
		host_ip.to_string(),
	]);
	IptablesRule::with_rule(NAT_TABLE, rule)
}

fn port_forward_rule(
	out_interface: &str,
	protocol: &str,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> IptablesRule {
	let mut rule = vec![
		TANGRAM_FORWARD_CHAIN.to_owned(),
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
	];
	append_comment(&mut rule, comment);
	rule.extend(["-j".to_owned(), "ACCEPT".to_owned()]);
	IptablesRule::with_rule(FILTER_TABLE, rule)
}

fn append_comment(rule: &mut Vec<String>, comment: &str) {
	rule.extend([
		"-m".to_owned(),
		"comment".to_owned(),
		"--comment".to_owned(),
		comment.to_owned(),
	]);
}

fn sandbox_rule_comment(index: u64) -> String {
	format!("{TANGRAM_RULE_COMMENT_PREFIX}{index}")
}

fn iptables_args<'a>(table: &'a [&'a str], operation: &'a str) -> Vec<&'a str> {
	let mut args = Vec::with_capacity(table.len() + 2);
	args.push(IPTABLES_WAIT);
	args.extend_from_slice(table);
	args.push(operation);
	args
}

fn insert_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
	let mut insert = iptables_args(table, "-I");
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
		let mut args = iptables_args(table, "-D");
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

fn delete_iptables_rule_once(table: &[&str], rule: &[&str]) -> tg::Result<bool> {
	let mut args = iptables_args(table, "-D");
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
				return Ok(false);
			}
			return Err(tg::error!(!error, "failed to spawn iptables"));
		},
	};
	if output.status.success() {
		return Ok(true);
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if is_iptables_permission_error(&stderr) {
		tracing::warn!(rule = %rule.join(" "), %stderr, "iptables cleanup denied");
	}
	Ok(false)
}

fn delete_iptables_rules_by_comment(table: &[&str], chain: &str, comment: &str) -> tg::Result<()> {
	let mut list = iptables_args(table, "-L");
	list.push(chain);
	list.extend(["--line-numbers", "-n", "-v"]);
	let output = match std::process::Command::new("iptables")
		.args(&list)
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
			tracing::warn!(%chain, %stderr, "iptables cleanup denied");
		}
		return Ok(());
	}
	let stdout = String::from_utf8_lossy(&output.stdout);
	let mut rule_numbers = Vec::new();
	for line in stdout.lines() {
		if !line.contains(comment) {
			continue;
		}
		let Some(number) = line
			.split_whitespace()
			.next()
			.and_then(|number| number.parse::<u32>().ok())
		else {
			continue;
		};
		rule_numbers.push(number);
	}
	for number in rule_numbers.into_iter().rev() {
		let number = number.to_string();
		let mut delete = iptables_args(table, "-D");
		delete.push(chain);
		delete.push(&number);
		let output = match std::process::Command::new("iptables")
			.args(&delete)
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
			tracing::warn!(%chain, %number, %stderr, "iptables cleanup denied");
			return Ok(());
		}
	}
	Ok(())
}

fn delete_bridge_masquerade_rules(bridge: &str) -> tg::Result<()> {
	let output = match std::process::Command::new("iptables")
		.args([IPTABLES_WAIT, "-t", "nat", "-S", "POSTROUTING"])
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
		let mut args = vec![IPTABLES_WAIT, "-t", "nat", "-D", "POSTROUTING"];
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

fn is_iptables_chain_exists(stderr: &str) -> bool {
	stderr.to_ascii_lowercase().contains("chain already exists")
}

impl Drop for IptablesRuleGuard {
	fn drop(&mut self) {
		let rule_args = self
			.rule
			.rule
			.iter()
			.map(String::as_str)
			.collect::<Vec<_>>();
		match delete_iptables_rule_once(self.rule.table, &rule_args) {
			Ok(true) => {},
			Ok(false) => {
				if let (Some(chain), Some(comment)) = (self.rule.chain(), self.rule.comment())
					&& let Err(error) =
						delete_iptables_rules_by_comment(self.rule.table, chain, comment)
				{
					tracing::error!(
						%error,
						"failed to clean up the sandbox port forwarding rule"
					);
				}
			},
			Err(error) => {
				tracing::error!(%error, "failed to clean up the sandbox port forwarding rule");
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bridge_filter_rules_restrict_private_and_inbound_traffic() {
		let dns = Ipv4Addr::new(10, 0, 0, 2);
		let rules = bridge_filter_rules("tangram0", &[dns])
			.into_iter()
			.map(|rule| rule.rule.join(" "))
			.collect::<Vec<_>>();

		assert_eq!(
			rules[1],
			"TANGRAM-BRIDGE -i tangram0 -d 10.0.0.2 -p tcp --dport 53 -j ACCEPT"
		);
		assert_eq!(
			rules[2],
			"TANGRAM-BRIDGE -i tangram0 -d 10.0.0.2 -p udp --dport 53 -j ACCEPT"
		);
		assert!(
			rules
				.iter()
				.any(|rule| rule == "TANGRAM-BRIDGE -i tangram0 -d 169.254.0.0/16 -j DROP")
		);
		assert!(
			rules
				.iter()
				.any(|rule| rule == "TANGRAM-BRIDGE -i tangram0 -d 172.16.0.0/12 -j DROP")
		);
		assert_eq!(
			rules[rules.len() - 2],
			"TANGRAM-BRIDGE -o tangram0 -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
		);
		assert_eq!(rules[rules.len() - 1], "TANGRAM-BRIDGE -o tangram0 -j DROP");
	}

	#[test]
	fn bridge_input_rules_allow_replies_and_configured_dns() {
		let dns = Ipv4Addr::new(192, 168, 1, 1);
		let rules = bridge_input_rules("tangram0", &[dns])
			.into_iter()
			.map(|rule| rule.rule.join(" "))
			.collect::<Vec<_>>();

		assert_eq!(
			rules,
			[
				"TANGRAM-INPUT -i tangram0 -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT",
				"TANGRAM-INPUT -i tangram0 -d 192.168.1.1 -p tcp --dport 53 -j ACCEPT",
				"TANGRAM-INPUT -i tangram0 -d 192.168.1.1 -p udp --dport 53 -j ACCEPT",
				"TANGRAM-INPUT -i tangram0 -j DROP",
			]
		);
	}
}
