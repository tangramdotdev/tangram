use {
	std::{
		collections::BTreeSet,
		net::Ipv4Addr,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
		sync::Mutex,
	},
	tangram_client::prelude::*,
};

const DYNAMIC_RULE_COMMENT_PREFIX: &str = "tangram:identity=";
const FORWARD_CHAIN: &str = "forward";
const INPUT_CHAIN: &str = "input";
const LEGACY_DYNAMIC_RULE_COMMENT_PREFIX: &str = "tangram:path=";
const NFT_TABLE: &str = "tangram";
const OUTPUT_CHAIN: &str = "output";
const POSTROUTING_CHAIN: &str = "postrouting";
const PREROUTING_CHAIN: &str = "prerouting";
const TANGRAM_BRIDGE_FORWARD_IN_COMMENT: &str = "tangram:bridge:forward-in";
const TANGRAM_BRIDGE_FORWARD_IN_DROP_COMMENT: &str = "tangram:bridge:forward-in-drop";
const TANGRAM_BRIDGE_FORWARD_OUT_COMMENT: &str = "tangram:bridge:forward-out";
const TANGRAM_BRIDGE_FORWARD_OUT_DNS_COMMENT_PREFIX: &str = "tangram:bridge:forward-out-dns:";
const TANGRAM_BRIDGE_FORWARD_OUT_PRIVATE_COMMENT: &str = "tangram:bridge:forward-out-private";
const TANGRAM_BRIDGE_FORWARD_REPLY_COMMENT: &str = "tangram:bridge:forward-reply";
const TANGRAM_BRIDGE_INPUT_DNS_COMMENT_PREFIX: &str = "tangram:bridge:input-dns:";
const TANGRAM_BRIDGE_INPUT_DROP_COMMENT: &str = "tangram:bridge:input-drop";
const TANGRAM_BRIDGE_INPUT_REPLY_COMMENT: &str = "tangram:bridge:input-reply";
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
		Self::with_operation("add", chain, comment, rule)
	}

	fn insert(chain: &'static str, comment: String, rule: Vec<String>) -> Self {
		Self::with_operation("insert", chain, comment, rule)
	}

	fn with_operation(
		operation: &str,
		chain: &'static str,
		comment: String,
		rule: Vec<String>,
	) -> Self {
		let mut args = vec![
			operation.to_owned(),
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

pub(crate) fn setup_bridge_networking(
	bridge: &str,
	addr: Ipv4Addr,
	dns: &[Ipv4Addr],
) -> tg::Result<()> {
	let octets = addr.octets();
	let subnet = Ipv4Addr::new(octets[0], octets[1], 0, 0);
	let cidr = format!("{subnet}/16");
	setup_firewall()?;
	replace_nft_rule(
		POSTROUTING_CHAIN,
		TANGRAM_BRIDGE_MASQUERADE_COMMENT,
		bridge_masquerade_rule(bridge, &cidr),
	)?;
	delete_rules_by_comment(
		INPUT_CHAIN,
		CommentMatcher::Prefix(TANGRAM_BRIDGE_INPUT_DNS_COMMENT_PREFIX),
	)?;
	delete_rules_by_comment(
		FORWARD_CHAIN,
		CommentMatcher::Prefix(TANGRAM_BRIDGE_FORWARD_OUT_DNS_COMMENT_PREFIX),
	)?;
	for (index, addr) in dns.iter().enumerate() {
		for protocol in ["tcp", "udp"] {
			let comment = format!("{TANGRAM_BRIDGE_INPUT_DNS_COMMENT_PREFIX}{index}:{protocol}");
			run_nft_checked(&nft_add_rule_args(
				INPUT_CHAIN,
				dns_rule(bridge, *addr, protocol, &comment),
			))?;
			let comment =
				format!("{TANGRAM_BRIDGE_FORWARD_OUT_DNS_COMMENT_PREFIX}{index}:{protocol}");
			run_nft_checked(&nft_add_rule_args(
				FORWARD_CHAIN,
				dns_rule(bridge, *addr, protocol, &comment),
			))?;
		}
	}
	replace_nft_rule(
		INPUT_CHAIN,
		TANGRAM_BRIDGE_INPUT_DROP_COMMENT,
		input_drop_rule(bridge, TANGRAM_BRIDGE_INPUT_DROP_COMMENT),
	)?;
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_BRIDGE_FORWARD_OUT_PRIVATE_COMMENT,
		forward_private_drop_rule(bridge, TANGRAM_BRIDGE_FORWARD_OUT_PRIVATE_COMMENT),
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
	replace_nft_rule(
		FORWARD_CHAIN,
		TANGRAM_BRIDGE_FORWARD_IN_DROP_COMMENT,
		forward_in_drop_rule(bridge, TANGRAM_BRIDGE_FORWARD_IN_DROP_COMMENT),
	)?;
	// Insert replies before all bridge drops, including rules retained from an earlier setup.
	for (chain, comment) in [
		(FORWARD_CHAIN, TANGRAM_BRIDGE_FORWARD_REPLY_COMMENT),
		(INPUT_CHAIN, TANGRAM_BRIDGE_INPUT_REPLY_COMMENT),
	] {
		delete_rules_by_comment(chain, CommentMatcher::Exact(comment))?;
		let rule = NftRule::insert(
			chain,
			comment.to_owned(),
			bridge_reply_rule(bridge, comment),
		);
		run_nft_checked(&rule.args)?;
	}

	Ok(())
}

pub(crate) fn add_port_forwarding_rules(
	index: u64,
	identity: &Path,
	out_interface: &str,
	host_ip: Ipv4Addr,
	guest_ip: Ipv4Addr,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Vec<FirewallRuleGuard>> {
	let identity = hash_identity(identity);
	setup_firewall()?;
	cleanup_dynamic_port_forwarding_rules(&identity)?;
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

fn cleanup_dynamic_port_forwarding_rules(identity: &str) -> tg::Result<()> {
	static CLEANED: std::sync::OnceLock<Mutex<BTreeSet<String>>> = std::sync::OnceLock::new();
	let cleaned = CLEANED.get_or_init(Mutex::default);
	if cleaned
		.lock()
		.map_err(|_| tg::error!("failed to lock the firewall cleanup state"))?
		.contains(identity)
	{
		return Ok(());
	}
	cleanup_dynamic_port_forwarding_rules_inner(identity)?;
	cleaned
		.lock()
		.map_err(|_| tg::error!("failed to lock the firewall cleanup state"))?
		.insert(identity.to_owned());
	Ok(())
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
		INPUT_CHAIN,
		"{ type filter hook input priority filter; policy accept; }",
	)?;
	ensure_nft_chain(
		FORWARD_CHAIN,
		"{ type filter hook forward priority filter; policy accept; }",
	)?;
	Ok(())
}

fn cleanup_dynamic_port_forwarding_rules_inner(identity: &str) -> tg::Result<()> {
	setup_firewall()?;
	for prefix in [
		dynamic_rule_comment_prefix(DYNAMIC_RULE_COMMENT_PREFIX, identity),
		dynamic_rule_comment_prefix(LEGACY_DYNAMIC_RULE_COMMENT_PREFIX, identity),
	] {
		for chain in [
			PREROUTING_CHAIN,
			OUTPUT_CHAIN,
			POSTROUTING_CHAIN,
			FORWARD_CHAIN,
		] {
			delete_rules_by_comment(chain, CommentMatcher::Prefix(&prefix))?;
		}
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

fn dns_rule(bridge: &str, addr: Ipv4Addr, protocol: &str, comment: &str) -> Vec<String> {
	vec![
		"iifname".to_owned(),
		quote(bridge),
		"ip".to_owned(),
		"daddr".to_owned(),
		addr.to_string(),
		protocol.to_owned(),
		"dport".to_owned(),
		"53".to_owned(),
		"accept".to_owned(),
		"comment".to_owned(),
		quote(comment),
	]
}

fn bridge_reply_rule(bridge: &str, comment: &str) -> Vec<String> {
	vec![
		"iifname".to_owned(),
		quote(bridge),
		"ct".to_owned(),
		"state".to_owned(),
		"established,related".to_owned(),
		"accept".to_owned(),
		"comment".to_owned(),
		quote(comment),
	]
}

fn input_drop_rule(bridge: &str, comment: &str) -> Vec<String> {
	vec![
		"iifname".to_owned(),
		quote(bridge),
		"drop".to_owned(),
		"comment".to_owned(),
		quote(comment),
	]
}

fn forward_private_drop_rule(bridge: &str, comment: &str) -> Vec<String> {
	vec![
		"iifname".to_owned(),
		quote(bridge),
		"ip".to_owned(),
		"daddr".to_owned(),
		"{ 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 198.18.0.0/15, 224.0.0.0/4, 240.0.0.0/4 }".to_owned(),
		"drop".to_owned(),
		"comment".to_owned(),
		quote(comment),
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

fn forward_in_drop_rule(interface: &str, comment: &str) -> Vec<String> {
	vec![
		"oifname".to_owned(),
		quote(interface),
		"drop".to_owned(),
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
	NftRule::insert(FORWARD_CHAIN, comment.to_owned(), rule)
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

fn sandbox_rule_comment(identity: &str, index: u64) -> String {
	format!(
		"{}sandbox_index={index}",
		dynamic_rule_comment_prefix(DYNAMIC_RULE_COMMENT_PREFIX, identity)
	)
}

fn dynamic_rule_comment_prefix(prefix: &str, identity: &str) -> String {
	format!("{prefix}{identity}:")
}

fn hash_identity(identity: &Path) -> String {
	let identity = canonicalize_identity(identity);
	let mut hash = 0xcbf2_9ce4_8422_2325_u64;
	for byte in identity.as_os_str().as_bytes() {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
	}
	format!("{hash:016x}")
}

fn canonicalize_identity(identity: &Path) -> PathBuf {
	std::fs::canonicalize(identity).unwrap_or_else(|_| identity.to_owned())
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bridge_rules_restrict_private_and_host_traffic() {
		let private = forward_private_drop_rule("tangram0", "private").join(" ");
		let input = input_drop_rule("tangram0", "input").join(" ");

		assert!(private.contains("169.254.0.0/16"));
		assert!(private.contains("172.16.0.0/12"));
		assert!(private.starts_with("iifname \"tangram0\" ip daddr"));
		assert_eq!(input, "iifname \"tangram0\" drop comment \"input\"");
	}

	#[test]
	fn dns_rule_only_allows_port_fifty_three() {
		let addr = Ipv4Addr::new(10, 0, 0, 2);
		let rule = dns_rule("tangram0", addr, "udp", "dns").join(" ");

		assert_eq!(
			rule,
			"iifname \"tangram0\" ip daddr 10.0.0.2 udp dport 53 accept comment \"dns\""
		);
	}

	#[test]
	fn published_port_rule_precedes_inbound_drop() {
		let guest = Ipv4Addr::new(172, 18, 0, 4);
		let rule = port_forward_rule("tangram0", "tcp", guest, 8080, "port");

		assert_eq!(rule.args[0], "insert");
		assert!(rule.args.join(" ").contains("tcp dport 8080 accept"));
	}
}
