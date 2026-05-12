use {std::net::Ipv4Addr, tangram_client::prelude::*};

const DYNAMIC_RULE_COMMENT_PREFIX: &str = "tangram:sandbox=";
const FORWARD_CHAIN: &str = "forward";
const NFT_TABLE: &str = "tangram";
const OUTPUT_CHAIN: &str = "output";
const POSTROUTING_CHAIN: &str = "postrouting";
const PREROUTING_CHAIN: &str = "prerouting";
const TANGRAM_BRIDGE_FORWARD_IN_COMMENT: &str = "tangram:bridge:forward-in";
const TANGRAM_BRIDGE_FORWARD_OUT_COMMENT: &str = "tangram:bridge:forward-out";
const TANGRAM_BRIDGE_MASQUERADE_COMMENT: &str = "tangram:bridge:masquerade";
const TANGRAM_TAP_FORWARD_IN_COMMENT: &str = "tangram:tap:forward-in";
const TANGRAM_TAP_FORWARD_OUT_COMMENT: &str = "tangram:tap:forward-out";
const TANGRAM_TAP_MASQUERADE_COMMENT: &str = "tangram:tap:masquerade";
pub(crate) const TAP_INTERFACE_NAME_PREFIX: &str = "tg-";

#[derive(Debug)]
struct NftRule {
	chain: &'static str,
	comment: String,
	args: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct FirewallRuleGuard {
	rule: NftRule,
}

#[derive(Clone, Copy)]
enum CommentMatcher<'a> {
	Exact(&'a str),
	Prefix(&'a str),
}

impl NftRule {
	fn new(chain: &'static str, comment: String, rule: Vec<String>) -> Self {
		let mut args = vec![
			"add".to_owned(),
			"rule".to_owned(),
			"ip".to_owned(),
			NFT_TABLE.to_owned(),
			chain.to_owned(),
		];
		args.extend(rule);
		Self {
			chain,
			comment,
			args,
		}
	}
}

impl FirewallRuleGuard {
	fn new(rule: NftRule) -> tg::Result<Self> {
		run_nft_checked(&rule.args)?;
		Ok(Self { rule })
	}
}

pub(crate) fn enable_ipv4_forwarding() -> tg::Result<()> {
	std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")
		.map_err(|error| tg::error!(!error, "failed to enable ipv4 forwarding"))
}

pub(crate) fn enable_route_localnet(interface: &str) -> tg::Result<()> {
	let path = format!("/proc/sys/net/ipv4/conf/{interface}/route_localnet");
	std::fs::write(&path, "1\n")
		.map_err(|error| tg::error!(!error, %path, "failed to enable route_localnet"))
}

pub(crate) fn setup_tap_networking() -> tg::Result<()> {
	let interface = format!("{TAP_INTERFACE_NAME_PREFIX}*");
	setup_firewall()?;
	replace_nft_rule(
		POSTROUTING_CHAIN,
		TANGRAM_TAP_MASQUERADE_COMMENT,
		tap_masquerade_rule(&interface),
	)?;
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_TAP_FORWARD_OUT_COMMENT,
		forward_out_rule(&interface, TANGRAM_TAP_FORWARD_OUT_COMMENT),
	)?;
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_TAP_FORWARD_IN_COMMENT,
		forward_in_rule(&interface, TANGRAM_TAP_FORWARD_IN_COMMENT),
	)?;
	Ok(())
}

pub(crate) fn setup_bridge_networking(bridge: &str, addr: Ipv4Addr) -> tg::Result<()> {
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	setup_firewall()?;
	replace_nft_rule(
		POSTROUTING_CHAIN,
		TANGRAM_BRIDGE_MASQUERADE_COMMENT,
		bridge_masquerade_rule(bridge, &cidr),
	)?;
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_BRIDGE_FORWARD_OUT_COMMENT,
		forward_out_rule(bridge, TANGRAM_BRIDGE_FORWARD_OUT_COMMENT),
	)?;
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_BRIDGE_FORWARD_IN_COMMENT,
		forward_in_rule(bridge, TANGRAM_BRIDGE_FORWARD_IN_COMMENT),
	)?;
	Ok(())
}

pub(crate) fn add_port_forwarding_rules(
	id: &tg::sandbox::Id,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<FirewallRuleGuard>> {
	setup_firewall()?;
	cleanup_dynamic_port_forwarding_rules()?;
	let mut guards = Vec::new();
	let comment = sandbox_rule_comment(id);
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
		guards.push(FirewallRuleGuard::new(port_dnat_rule(
			PREROUTING_CHAIN,
			protocol,
			port.host_ip,
			host.start,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
		guards.push(FirewallRuleGuard::new(port_dnat_rule(
			OUTPUT_CHAIN,
			protocol,
			port.host_ip,
			host.start,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
		guards.push(FirewallRuleGuard::new(port_forward_rule(
			out_interface,
			protocol,
			guest_ip,
			port.guest.start,
			&comment,
		))?);
		guards.push(FirewallRuleGuard::new(port_snat_rule(
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

fn setup_firewall() -> tg::Result<()> {
	static SETUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	SETUP.get_or_init(setup_firewall_inner).clone()
}

fn cleanup_dynamic_port_forwarding_rules() -> tg::Result<()> {
	static CLEANUP: std::sync::OnceLock<tg::Result<()>> = std::sync::OnceLock::new();
	CLEANUP
		.get_or_init(cleanup_dynamic_port_forwarding_rules_inner)
		.clone()
}

fn setup_firewall_inner() -> tg::Result<()> {
	ensure_nft_table()?;
	ensure_nft_chain(
		PREROUTING_CHAIN,
		"{ type nat hook prerouting priority -101; policy accept; }",
	)?;
	ensure_nft_chain(
		OUTPUT_CHAIN,
		"{ type nat hook output priority -101; policy accept; }",
	)?;
	ensure_nft_chain(
		POSTROUTING_CHAIN,
		"{ type nat hook postrouting priority 99; policy accept; }",
	)?;
	ensure_nft_chain(
		FORWARD_CHAIN,
		"{ type filter hook forward priority filter; policy accept; }",
	)?;
	Ok(())
}

fn cleanup_dynamic_port_forwarding_rules_inner() -> tg::Result<()> {
	setup_firewall()?;
	for chain in [
		PREROUTING_CHAIN,
		OUTPUT_CHAIN,
		POSTROUTING_CHAIN,
		FORWARD_CHAIN,
	] {
		delete_rules_by_comment(chain, CommentMatcher::Prefix(DYNAMIC_RULE_COMMENT_PREFIX))?;
	}
	Ok(())
}

fn ensure_nft_table() -> tg::Result<()> {
	if run_nft(&["list", "table", "ip", NFT_TABLE])?
		.status
		.success()
	{
		return Ok(());
	}
	let args = ["add", "table", "ip", NFT_TABLE];
	let output = run_nft(&args)?;
	if output.status.success() {
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if is_nft_already_exists_error(&stderr) {
		return Ok(());
	}
	Err(nft_error(&args, &stderr))
}

fn ensure_nft_chain(chain: &'static str, spec: &str) -> tg::Result<()> {
	if run_nft(&["list", "chain", "ip", NFT_TABLE, chain])?
		.status
		.success()
	{
		return Ok(());
	}
	let args = ["add", "chain", "ip", NFT_TABLE, chain, spec];
	let output = run_nft(&args)?;
	if output.status.success() {
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if is_nft_already_exists_error(&stderr) {
		return Ok(());
	}
	Err(nft_error(&args, &stderr))
}

fn replace_nft_rule(
	chain: &'static str,
	comment: &'static str,
	rule: Vec<String>,
) -> tg::Result<()> {
	delete_rules_by_comment(chain, CommentMatcher::Exact(comment))?;
	run_nft_checked(&nft_add_rule_args(chain, rule))
}

fn tap_masquerade_rule(interface: &str) -> Vec<String> {
	vec![
		"ip".to_owned(),
		"saddr".to_owned(),
		"172.16.0.0/12".to_owned(),
		"oifname".to_owned(),
		"!=".to_owned(),
		quote(interface),
		"masquerade".to_owned(),
		"comment".to_owned(),
		quote(TANGRAM_TAP_MASQUERADE_COMMENT),
	]
}

fn bridge_masquerade_rule(bridge: &str, cidr: &str) -> Vec<String> {
	vec![
		"ip".to_owned(),
		"saddr".to_owned(),
		cidr.to_owned(),
		"oifname".to_owned(),
		"!=".to_owned(),
		quote(bridge),
		"masquerade".to_owned(),
		"comment".to_owned(),
		quote(TANGRAM_BRIDGE_MASQUERADE_COMMENT),
	]
}

fn forward_out_rule(interface: &str, comment: &str) -> Vec<String> {
	vec![
		"iifname".to_owned(),
		quote(interface),
		"accept".to_owned(),
		"comment".to_owned(),
		quote(comment),
	]
}

fn forward_in_rule(interface: &str, comment: &str) -> Vec<String> {
	vec![
		"oifname".to_owned(),
		quote(interface),
		"ct".to_owned(),
		"state".to_owned(),
		"established,related".to_owned(),
		"accept".to_owned(),
		"comment".to_owned(),
		quote(comment),
	]
}

fn port_dnat_rule(
	chain: &'static str,
	protocol: &str,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> NftRule {
	let mut rule = vec![
		protocol.to_owned(),
		"dport".to_owned(),
		host_port.to_string(),
	];
	if let Some(host_ip) = host_ip.filter(|host_ip| !host_ip.is_unspecified()) {
		rule.extend(["ip".to_owned(), "daddr".to_owned(), host_ip.to_string()]);
	} else {
		rule.extend([
			"fib".to_owned(),
			"daddr".to_owned(),
			"type".to_owned(),
			"local".to_owned(),
		]);
	}
	rule.extend([
		"dnat".to_owned(),
		"to".to_owned(),
		format!("{guest_ip}:{guest_port}"),
		"comment".to_owned(),
		quote(comment),
	]);
	NftRule::new(chain, comment.to_owned(), rule)
}

fn port_snat_rule(
	out_interface: &str,
	protocol: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> NftRule {
	let rule = vec![
		"oifname".to_owned(),
		quote(out_interface),
		"ip".to_owned(),
		"daddr".to_owned(),
		guest_ip.to_string(),
		protocol.to_owned(),
		"dport".to_owned(),
		guest_port.to_string(),
		"fib".to_owned(),
		"saddr".to_owned(),
		"type".to_owned(),
		"local".to_owned(),
		"snat".to_owned(),
		"to".to_owned(),
		host_ip.to_string(),
		"comment".to_owned(),
		quote(comment),
	];
	NftRule::new(POSTROUTING_CHAIN, comment.to_owned(), rule)
}

fn port_forward_rule(
	out_interface: &str,
	protocol: &str,
	guest_ip: Ipv4Addr,
	guest_port: u16,
	comment: &str,
) -> NftRule {
	let rule = vec![
		"oifname".to_owned(),
		quote(out_interface),
		"ip".to_owned(),
		"daddr".to_owned(),
		guest_ip.to_string(),
		protocol.to_owned(),
		"dport".to_owned(),
		guest_port.to_string(),
		"accept".to_owned(),
		"comment".to_owned(),
		quote(comment),
	];
	NftRule::new(FORWARD_CHAIN, comment.to_owned(), rule)
}

fn nft_add_rule_args(chain: &'static str, rule: Vec<String>) -> Vec<String> {
	let mut args = vec![
		"add".to_owned(),
		"rule".to_owned(),
		"ip".to_owned(),
		NFT_TABLE.to_owned(),
		chain.to_owned(),
	];
	args.extend(rule);
	args
}

fn delete_rules_by_comment(chain: &str, matcher: CommentMatcher<'_>) -> tg::Result<()> {
	let output = run_nft(&["-a", "list", "chain", "ip", NFT_TABLE, chain])?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		if is_nft_missing_table_or_chain_error(&stderr) {
			return Ok(());
		}
		return Err(nft_error(
			&["-a", "list", "chain", "ip", NFT_TABLE, chain],
			&stderr,
		));
	}
	let stdout = String::from_utf8_lossy(&output.stdout);
	let handles = stdout
		.lines()
		.filter(|line| matcher.matches(line))
		.filter_map(rule_handle)
		.map(str::to_owned)
		.collect::<Vec<_>>();
	for handle in handles {
		run_nft_checked(&[
			"delete".to_owned(),
			"rule".to_owned(),
			"ip".to_owned(),
			NFT_TABLE.to_owned(),
			chain.to_owned(),
			"handle".to_owned(),
			handle,
		])?;
	}
	Ok(())
}

fn rule_handle(line: &str) -> Option<&str> {
	let (_, handle) = line.rsplit_once("# handle ")?;
	handle.split_whitespace().next()
}

impl CommentMatcher<'_> {
	fn matches(&self, line: &str) -> bool {
		match self {
			Self::Exact(comment) => line.contains(&format!("comment {}", quote(comment))),
			Self::Prefix(prefix) => line.contains(&format!("comment \"{prefix}")),
		}
	}
}

fn sandbox_rule_comment(id: &tg::sandbox::Id) -> String {
	format!("{DYNAMIC_RULE_COMMENT_PREFIX}{id}")
}

fn quote(value: &str) -> String {
	format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn run_nft(args: &[&str]) -> tg::Result<std::process::Output> {
	std::process::Command::new("nft")
		.args(args)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn nft"))
}

fn run_nft_checked(args: &[String]) -> tg::Result<()> {
	let output = std::process::Command::new("nft")
		.args(args)
		.stderr(std::process::Stdio::piped())
		.output()
		.map_err(|error| tg::error!(!error, "failed to spawn nft"))?;
	if output.status.success() {
		return Ok(());
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	Err(nft_error(args, &stderr))
}

fn nft_error<S>(args: &[S], stderr: &str) -> tg::Error
where
	S: AsRef<str>,
{
	let command = std::iter::once("nft")
		.chain(args.iter().map(AsRef::as_ref))
		.collect::<Vec<_>>()
		.join(" ");
	tg::error!(%command, %stderr, "failed to run nft")
}

fn is_nft_already_exists_error(stderr: &str) -> bool {
	stderr.to_ascii_lowercase().contains("file exists")
}

fn is_nft_missing_table_or_chain_error(stderr: &str) -> bool {
	let stderr = stderr.to_ascii_lowercase();
	stderr.contains("no such file or directory") || stderr.contains("no such file")
}

impl Drop for FirewallRuleGuard {
	fn drop(&mut self) {
		if let Err(error) =
			delete_rules_by_comment(self.rule.chain, CommentMatcher::Exact(&self.rule.comment))
		{
			tracing::error!(%error, "failed to clean up the sandbox port forwarding rule");
		}
	}
}
