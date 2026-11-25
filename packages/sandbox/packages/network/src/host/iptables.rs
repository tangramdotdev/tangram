use {
	std::{
		collections::BTreeSet,
		io::Write as _,
		net::Ipv4Addr,
		path::Path,
		sync::{Mutex, OnceLock},
	},
	tangram_client::prelude::*,
};

const FILTER_TABLE: &[&str] = &[];
const IPTABLES_WAIT: &str = "-w";
const NAT_TABLE: &[&str] = &["-t", "nat"];
const TANGRAM_DNAT_CHAIN: &str = "TANGRAM-DNAT";
const TANGRAM_BRIDGE_CHAIN: &str = "TANGRAM-BRIDGE";
const TANGRAM_FORWARD_CHAIN: &str = "TANGRAM-FWD";
const TANGRAM_INPUT_CHAIN: &str = "TANGRAM-INPUT";
const TANGRAM_SNAT_CHAIN: &str = "TANGRAM-SNAT";
const TANGRAM_RULE_COMMENT_PREFIX: &str = "tangram:identity=";

#[derive(Debug)]
struct IptablesRule {
	table: &'static [&'static str],
	rule: Vec<String>,
}

#[derive(Debug)]
pub struct IptablesRuleGuard {
	rule: IptablesRule,
}

#[derive(Clone, Copy)]
enum CommentMatcher<'a> {
	Exact(&'a str),
	Prefix(&'a str),
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

impl CommentMatcher<'_> {
	fn matches(&self, comment: &str) -> bool {
		match self {
			Self::Exact(expected) => comment == *expected,
			Self::Prefix(prefix) => comment.starts_with(prefix),
		}
	}
}

/// Remove the rules that earlier versions of the server and the `nft` backend installed in the shared chains for a bridge. If the `iptables` binary reports a permission error, log a warning and return Ok rather than failing; the server can still start without networking, and the operator may not have `CAP_NET_ADMIN`.
fn delete_forward_accept_rules(bridge: &str) -> tg::Result<()> {
	for rule in forward_accept_rules(bridge) {
		let rule_args = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		delete_iptables_rule(rule.table, &rule_args)?;
	}
	Ok(())
}

/// Accept the traffic of an interface in the `FORWARD` chain, for an interface whose traffic is filtered by another table. Only replies and published ports are accepted toward the interface.
pub fn accept_forwarded_traffic(interface: &str) -> tg::Result<()> {
	for rule in forward_accept_rules(interface) {
		let rule_args = rule.rule.iter().map(String::as_str).collect::<Vec<_>>();
		get_or_set_iptables_rule(rule.table, &rule_args)?;
	}
	Ok(())
}

pub fn setup_tap_networking() -> tg::Result<()> {
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

pub fn setup_bridge_networking(bridge: &str, addr: Ipv4Addr, dns: &[Ipv4Addr]) -> tg::Result<()> {
	// Remove the legacy rules and set up the port forwarding chains.
	if let Err(error) = delete_forward_accept_rules(bridge) {
		tracing::warn!(%error, "failed to clean up persistent sandbox rules");
	}
	setup_port_forwarding()?;

	// Install the masquerade rule, then remove the bridge's other masquerade rules.
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	let masquerade = [
		"POSTROUTING",
		"-s",
		cidr.as_str(),
		"!",
		"-o",
		bridge,
		"-j",
		"MASQUERADE",
	];
	get_or_set_iptables_rule(NAT_TABLE, &masquerade)?;
	if let Err(error) = delete_bridge_masquerade_rules(bridge, &masquerade[1..]) {
		tracing::warn!(%error, "failed to clean up persistent sandbox rules");
	}

	// Replace the bridge chains and the jumps to them in one transaction, so that no rule is missing while they are replaced.
	let forward = list_iptables_rules(FILTER_TABLE, "FORWARD")?;
	let input = list_iptables_rules(FILTER_TABLE, "INPUT")?;
	let script = bridge_restore_script(bridge, dns, &forward, &input);
	restore_iptables(&script)?;

	Ok(())
}

/// Create the `iptables-restore` script that replaces the bridge chains, and that moves the jumps to the bridge and published port chains to the top of the built-in chains, given the current rules of the built-in chains.
fn bridge_restore_script(
	bridge: &str,
	dns: &[Ipv4Addr],
	forward: &[String],
	input: &[String],
) -> String {
	// Declare the bridge chains, which flushes them.
	let mut lines = vec![
		"*filter".to_owned(),
		format!(":{TANGRAM_BRIDGE_CHAIN} - [0:0]"),
		format!(":{TANGRAM_INPUT_CHAIN} - [0:0]"),
	];

	// Append the bridge rules.
	for rule in bridge_filter_rules(bridge, dns)
		.into_iter()
		.chain(bridge_input_rules(bridge, dns))
	{
		lines.push(format!("-A {}", rule.rule.join(" ")));
	}

	// Move the jumps to the top. Published ports must be considered before the bridge chain rejects unsolicited inbound traffic.
	let jumps = [
		(
			forward,
			format!("FORWARD -i {bridge} -j {TANGRAM_BRIDGE_CHAIN}"),
		),
		(
			forward,
			format!("FORWARD -o {bridge} -j {TANGRAM_BRIDGE_CHAIN}"),
		),
		(input, format!("INPUT -i {bridge} -j {TANGRAM_INPUT_CHAIN}")),
		(forward, format!("FORWARD -j {TANGRAM_FORWARD_CHAIN}")),
	];
	for (rules, jump) in jumps {
		let existing = format!("-A {jump}");
		for _ in rules.iter().filter(|rule| **rule == existing) {
			lines.push(format!("-D {jump}"));
		}
		lines.push(format!("-I {jump}"));
	}
	lines.push("COMMIT".to_owned());

	let mut script = lines.join("\n");
	script.push('\n');
	script
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

pub fn setup_port_forwarding() -> tg::Result<()> {
	static SETUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	SETUP.get_or_init(setup_port_forwarding_inner).clone()
}

/// Remove the port forwarding rules that this server left behind in an earlier run, once per server.
fn cleanup_port_forwarding_rules(identity: &str) -> tg::Result<()> {
	// Hold the lock while removing the rules, so that the server does not add rules that the removal deletes.
	static CLEANED: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
	let mut cleaned = CLEANED
		.get_or_init(Mutex::default)
		.lock()
		.map_err(|_| tg::error!("failed to lock the firewall cleanup state"))?;
	if cleaned.contains(identity) {
		return Ok(());
	}

	// Remove the rules whose comments name this server.
	let prefix = rule_comment_prefix(identity);
	for (table, chain) in [
		(NAT_TABLE, TANGRAM_DNAT_CHAIN),
		(NAT_TABLE, TANGRAM_SNAT_CHAIN),
		(FILTER_TABLE, TANGRAM_FORWARD_CHAIN),
	] {
		delete_iptables_rules_by_comment(table, chain, CommentMatcher::Prefix(&prefix))?;
	}
	cleaned.insert(identity.to_owned());

	Ok(())
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

fn forward_accept_rules(interface: &str) -> Vec<IptablesRule> {
	let mut rules = vec![IptablesRule::new(
		FILTER_TABLE,
		["FORWARD", "-i", interface, "-j", "ACCEPT"],
	)];
	for state in ["ESTABLISHED,RELATED", "DNAT"] {
		rules.push(IptablesRule::new(
			FILTER_TABLE,
			[
				"FORWARD",
				"-o",
				interface,
				"-m",
				"conntrack",
				"--ctstate",
				state,
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

pub fn get_or_set_iptables_rule(table: &[&str], rule: &[&str]) -> tg::Result<()> {
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

pub fn add_port_forwarding_rules(
	index: u64,
	identity: &Path,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<IptablesRuleGuard>> {
	let identity = super::hash_identity(identity);

	// Convert a trailing wildcard in the interface name to the wildcard that iptables matches.
	let out_interface = out_interface
		.strip_suffix('*')
		.map_or_else(|| out_interface.to_owned(), |prefix| format!("{prefix}+"));
	let out_interface = out_interface.as_str();

	setup_port_forwarding()?;
	cleanup_port_forwarding_rules(&identity)?;
	let mut guards = Vec::new();
	let comment = sandbox_rule_comment(&identity, index);
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

fn sandbox_rule_comment(identity: &str, index: u64) -> String {
	format!("{}sandbox_index={index}", rule_comment_prefix(identity))
}

fn rule_comment_prefix(identity: &str) -> String {
	format!("{TANGRAM_RULE_COMMENT_PREFIX}{identity}:")
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

/// Delete the rules in a chain whose comments match, in one transaction that names each rule by its specification, so that rules that other servers add or remove concurrently are not affected.
fn delete_iptables_rules_by_comment(
	table: &[&str],
	chain: &str,
	matcher: CommentMatcher<'_>,
) -> tg::Result<()> {
	let rules = list_iptables_rules(table, chain)?;
	let prefix = format!("-A {chain} ");
	let deletions = rules
		.iter()
		.filter_map(|rule| rule.strip_prefix(&prefix))
		.filter(|rule| rule_comment(rule).is_some_and(|comment| matcher.matches(&comment)))
		.map(|rule| format!("-D {chain} {rule}"))
		.collect::<Vec<_>>();
	if deletions.is_empty() {
		return Ok(());
	}
	let name = if table == NAT_TABLE { "nat" } else { "filter" };
	let mut script = format!("*{name}\n");
	for deletion in deletions {
		script.push_str(&deletion);
		script.push('\n');
	}
	script.push_str("COMMIT\n");
	restore_iptables(&script)?;
	Ok(())
}

/// List the rules of a chain in the format of `iptables -S`.
fn list_iptables_rules(table: &[&str], chain: &str) -> tg::Result<Vec<String>> {
	let mut list = iptables_args(table, "-S");
	list.push(chain);
	let output = std::process::Command::new("iptables")
		.args(&list)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(tg::error!(%stderr, %chain, "failed to list the iptables rules"));
	}
	let rules = String::from_utf8_lossy(&output.stdout)
		.lines()
		.map(str::to_owned)
		.collect();
	Ok(rules)
}

/// Apply an `iptables-restore` script without flushing the tables, which applies each table's changes in one transaction.
fn restore_iptables(script: &str) -> tg::Result<()> {
	// Spawn iptables-restore with the script on its standard input.
	let mut child = std::process::Command::new("iptables-restore")
		.args([IPTABLES_WAIT, "--noflush"])
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::piped())
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn iptables-restore"))?;
	let mut stdin = child.stdin.take().unwrap();
	stdin
		.write_all(script.as_bytes())
		.map_err(|error| tg::error!(!error, "failed to write the iptables-restore script"))?;
	drop(stdin);

	// Wait for iptables-restore to apply the script.
	let output = child
		.wait_with_output()
		.map_err(|error| tg::error!(!error, "failed to wait for iptables-restore"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(tg::error!(%script, %stderr, "failed to run iptables-restore"));
	}

	Ok(())
}

/// Get the comment of a rule in the format of `iptables -S`, which quotes a comment that contains characters other than letters, digits, hyphens, and underscores.
fn rule_comment(rule: &str) -> Option<String> {
	let (_, rest) = rule.split_once("--comment ")?;
	let Some(rest) = rest.strip_prefix('"') else {
		let comment = rest.split_whitespace().next()?;
		return Some(comment.to_owned());
	};
	let mut comment = String::new();
	let mut chars = rest.chars();
	loop {
		match chars.next()? {
			'"' => return Some(comment),
			'\\' => comment.push(chars.next()?),
			char => comment.push(char),
		}
	}
}

/// Delete the masquerade rules for a bridge, except for the rule to keep.
fn delete_bridge_masquerade_rules(bridge: &str, keep: &[&str]) -> tg::Result<()> {
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
		if rest.split_whitespace().eq(keep.iter().copied()) {
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
					&& let Err(error) = delete_iptables_rules_by_comment(
						self.rule.table,
						chain,
						CommentMatcher::Exact(comment),
					) {
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
	fn forward_accept_rules_only_accept_replies_and_published_ports_inbound() {
		let rules = forward_accept_rules("tangram0")
			.into_iter()
			.map(|rule| rule.rule.join(" "))
			.collect::<Vec<_>>();

		assert_eq!(
			rules,
			[
				"FORWARD -i tangram0 -j ACCEPT",
				"FORWARD -o tangram0 -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT",
				"FORWARD -o tangram0 -m conntrack --ctstate DNAT -j ACCEPT",
			]
		);
	}

	#[test]
	fn rule_comments_are_read_from_listed_rules() {
		let quoted = "-A TANGRAM-DNAT -d 127.0.0.1/32 -p tcp -m tcp --dport 1234 -m comment --comment \"tangram:identity=0123456789abcdef:sandbox_index=7\" -j DNAT --to-destination 172.18.0.5:8080";
		let unquoted = "-A TANGRAM-DNAT -p tcp -m comment --comment other -j ACCEPT";
		let escaped = r#"-A TANGRAM-DNAT -m comment --comment "a \"b\" c" -j ACCEPT"#;

		assert_eq!(
			rule_comment(quoted).as_deref(),
			Some("tangram:identity=0123456789abcdef:sandbox_index=7")
		);
		assert_eq!(rule_comment(unquoted).as_deref(), Some("other"));
		assert_eq!(rule_comment(escaped).as_deref(), Some("a \"b\" c"));
		assert_eq!(rule_comment("-A TANGRAM-DNAT -j ACCEPT"), None);

		let prefix = rule_comment_prefix("0123456789abcdef");
		let comment = rule_comment(quoted).unwrap();
		assert!(CommentMatcher::Prefix(&prefix).matches(&comment));
		assert!(
			!CommentMatcher::Prefix(&rule_comment_prefix("fedcba9876543210")).matches(&comment)
		);
		assert!(
			CommentMatcher::Exact(&sandbox_rule_comment("0123456789abcdef", 7)).matches(&comment)
		);
		assert!(
			!CommentMatcher::Exact(&sandbox_rule_comment("0123456789abcdef", 70)).matches(&comment)
		);
	}

	#[test]
	fn bridge_restore_script_moves_each_jump_once() {
		let forward = [
			"-P FORWARD DROP".to_owned(),
			"-A FORWARD -j DOCKER-USER".to_owned(),
			"-A FORWARD -i tangram0 -j TANGRAM-BRIDGE".to_owned(),
			"-A FORWARD -i tangram0 -j TANGRAM-BRIDGE".to_owned(),
			"-A FORWARD -j TANGRAM-FWD".to_owned(),
		];
		let script = bridge_restore_script("tangram0", &[], &forward, &[]);
		let lines = script.lines().collect::<Vec<_>>();

		assert_eq!(lines[0], "*filter");
		assert_eq!(lines[1], ":TANGRAM-BRIDGE - [0:0]");
		assert_eq!(lines[2], ":TANGRAM-INPUT - [0:0]");
		assert_eq!(
			lines[lines.len() - 8..],
			[
				"-D FORWARD -i tangram0 -j TANGRAM-BRIDGE",
				"-D FORWARD -i tangram0 -j TANGRAM-BRIDGE",
				"-I FORWARD -i tangram0 -j TANGRAM-BRIDGE",
				"-I FORWARD -o tangram0 -j TANGRAM-BRIDGE",
				"-I INPUT -i tangram0 -j TANGRAM-INPUT",
				"-D FORWARD -j TANGRAM-FWD",
				"-I FORWARD -j TANGRAM-FWD",
				"COMMIT",
			][..]
		);
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
