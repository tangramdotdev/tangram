use {
	neli::{
		ToBytes as _,
		consts::{
			nl::NlmF,
			rtnl::{
				Ifa, Iff, Ifla, IflaInfo, RtAddrFamily, RtScope, RtTable, Rta, Rtm, Rtn, Rtprot,
			},
			socket::NlFamily,
		},
		nl::NlPayload,
		router::synchronous::NlRouter,
		rtnl::{
			Ifaddrmsg, IfaddrmsgBuilder, Ifinfomsg, IfinfomsgBuilder, Rtattr, RtattrBuilder, Rtmsg,
			RtmsgBuilder,
		},
		types::{Buffer, RtBuffer},
		utils::Groups,
	},
	std::{ffi::CString, io::Cursor, net::Ipv4Addr},
	tangram_client as tg,
};

// VETH_INFO_PEER from <linux/veth.h>.
const VETH_INFO_PEER: libc::c_ushort = 1;

pub struct Netlink {
	rtnl: NlRouter,
}

impl std::fmt::Debug for Netlink {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Netlink").finish_non_exhaustive()
	}
}

impl Netlink {
	pub fn new() -> tg::Result<Self> {
		let (rtnl, _) = NlRouter::connect(NlFamily::Route, None, Groups::empty())
			.map_err(|source| tg::error!(!source, "failed to connect to rtnetlink"))?;
		rtnl.enable_strict_checking(true)
			.map_err(|source| tg::error!(!source, "failed to enable strict checking"))?;
		Ok(Self { rtnl })
	}

	pub fn link_rename(&mut self, old_name: &str, new_name: &str) -> tg::Result<()> {
		let index = index_of(old_name)?;
		let mut attrs = RtBuffer::<Ifla, Buffer>::new();
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Ifla::Ifname)
				.rta_payload(format!("{new_name}\0"))
				.build()
				.map_err(|source| tg::error!(!source, "failed to build the ifname attribute"))?,
		);
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.ifi_index(index)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::ACK, msg)
	}

	pub fn link_set_up(&mut self, name: &str) -> tg::Result<()> {
		let index = index_of(name)?;
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.ifi_index(index)
			.ifi_flags(Iff::UP)
			.ifi_change(Iff::UP)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::ACK, msg)
	}

	pub fn link_delete(&mut self, name: &str) -> tg::Result<()> {
		let index = index_of(name)?;
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.ifi_index(index)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Dellink, NlmF::ACK, msg)
	}

	#[expect(clippy::unused_self)]
	pub fn link_exists(&mut self, name: &str) -> tg::Result<bool> {
		let c = CString::new(name)
			.map_err(|source| tg::error!(!source, %name, "invalid interface name"))?;
		let index = unsafe { libc::if_nametoindex(c.as_ptr()) };
		if index != 0 {
			return Ok(true);
		}
		let errno = std::io::Error::last_os_error();
		if errno.raw_os_error() == Some(libc::ENODEV) {
			return Ok(false);
		}
		Err(tg::error!(source = errno, %name, "failed to look up the interface"))
	}

	pub fn link_add_bridge(&mut self, name: &str) -> tg::Result<()> {
		let ifname = rtattr_ifname(name)?;
		let linkinfo = rtattr_linkinfo_kind("bridge")?;
		let mut attrs = RtBuffer::<Ifla, Buffer>::new();
		attrs.push(ifname);
		attrs.push(linkinfo);
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::CREATE | NlmF::EXCL | NlmF::ACK, msg)
	}

	pub fn link_add_veth_pair(&mut self, host: &str, peer: &str) -> tg::Result<()> {
		// Build the peer's nested payload: an ifinfomsg followed by an IFLA_IFNAME attribute.
		let peer_ifname = rtattr_ifname(peer)?;
		let mut peer_attrs = RtBuffer::<Ifla, Buffer>::new();
		peer_attrs.push(peer_ifname);
		let peer_msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.rtattrs(peer_attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the peer ifinfomsg"))?;
		let mut peer_buf = Cursor::new(Vec::<u8>::new());
		peer_msg
			.to_bytes(&mut peer_buf)
			.map_err(|source| tg::error!(!source, "failed to serialize the peer ifinfomsg"))?;

		// Wrap the peer payload in a VETH_INFO_PEER attribute.
		let veth_peer: Rtattr<libc::c_ushort, Buffer> = RtattrBuilder::default()
			.rta_type(VETH_INFO_PEER)
			.rta_payload(peer_buf.into_inner())
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the veth peer attribute"))?;

		// Build IFLA_LINKINFO containing IFLA_INFO_KIND = "veth" and IFLA_INFO_DATA nesting the peer.
		let info_kind: Rtattr<IflaInfo, Buffer> = RtattrBuilder::default()
			.rta_type(IflaInfo::Kind)
			.rta_payload("veth\0".to_owned())
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the info_kind attribute"))?;
		let info_data: Rtattr<IflaInfo, Buffer> = RtattrBuilder::default()
			.rta_type(IflaInfo::Data)
			.rta_payload(Vec::<u8>::new())
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the info_data attribute"))?
			.nest(&veth_peer)
			.map_err(|source| tg::error!(!source, "failed to nest the veth peer"))?;
		let linkinfo: Rtattr<Ifla, Buffer> = RtattrBuilder::default()
			.rta_type(Ifla::Linkinfo)
			.rta_payload(Vec::<u8>::new())
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the linkinfo attribute"))?
			.nest(&info_kind)
			.map_err(|source| tg::error!(!source, "failed to nest the info_kind"))?
			.nest(&info_data)
			.map_err(|source| tg::error!(!source, "failed to nest the info_data"))?;

		let mut attrs = RtBuffer::<Ifla, Buffer>::new();
		attrs.push(rtattr_ifname(host)?);
		attrs.push(linkinfo);
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::CREATE | NlmF::EXCL | NlmF::ACK, msg)
	}

	pub fn link_set_master(&mut self, name: &str, master: &str) -> tg::Result<()> {
		let index = index_of(name)?;
		let master_index = u32::try_from(index_of(master)?)
			.map_err(|source| tg::error!(!source, "master interface index overflow"))?;
		let mut attrs = RtBuffer::<Ifla, Buffer>::new();
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Ifla::Master)
				.rta_payload(master_index)
				.build()
				.map_err(|source| tg::error!(!source, "failed to build the master attribute"))?,
		);
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.ifi_index(index)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::ACK, msg)
	}

	pub fn link_set_netns_pid(&mut self, name: &str, pid: u32) -> tg::Result<()> {
		let index = index_of(name)?;
		let mut attrs = RtBuffer::<Ifla, Buffer>::new();
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Ifla::NetNsPid)
				.rta_payload(pid)
				.build()
				.map_err(|source| {
					tg::error!(!source, "failed to build the net_ns_pid attribute")
				})?,
		);
		let msg = IfinfomsgBuilder::default()
			.ifi_family(RtAddrFamily::Unspecified)
			.ifi_index(index)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifinfomsg"))?;
		self.send::<Ifinfomsg>(Rtm::Newlink, NlmF::ACK, msg)
	}

	pub fn addr_add_v4(&mut self, name: &str, addr: Ipv4Addr, prefix_len: u8) -> tg::Result<()> {
		self.addr_set_v4(
			name,
			addr,
			prefix_len,
			NlmF::CREATE | NlmF::EXCL | NlmF::ACK,
		)
	}

	pub fn addr_replace_v4(
		&mut self,
		name: &str,
		addr: Ipv4Addr,
		prefix_len: u8,
	) -> tg::Result<()> {
		self.addr_set_v4(
			name,
			addr,
			prefix_len,
			NlmF::CREATE | NlmF::REPLACE | NlmF::ACK,
		)
	}

	fn addr_set_v4(
		&mut self,
		name: &str,
		addr: Ipv4Addr,
		prefix_len: u8,
		flags: NlmF,
	) -> tg::Result<()> {
		let index = index_of(name)?;
		let ifa_index = u32::try_from(index)
			.map_err(|source| tg::error!(!source, "interface index overflow"))?;
		let bytes = addr.octets().to_vec();
		let mut attrs = RtBuffer::<Ifa, Buffer>::new();
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Ifa::Local)
				.rta_payload(bytes.clone())
				.build()
				.map_err(|source| tg::error!(!source, "failed to build the ifa_local attribute"))?,
		);
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Ifa::Address)
				.rta_payload(bytes)
				.build()
				.map_err(|source| {
					tg::error!(!source, "failed to build the ifa_address attribute")
				})?,
		);
		let msg = IfaddrmsgBuilder::default()
			.ifa_family(RtAddrFamily::Inet)
			.ifa_prefixlen(prefix_len)
			.ifa_scope(RtScope::Universe)
			.ifa_index(ifa_index)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the ifaddrmsg"))?;
		self.send::<Ifaddrmsg>(Rtm::Newaddr, flags, msg)
	}

	pub fn route_add_default_v4(&mut self, gateway: Ipv4Addr) -> tg::Result<()> {
		let bytes = gateway.octets().to_vec();
		let mut attrs = RtBuffer::<Rta, Buffer>::new();
		attrs.push(
			RtattrBuilder::default()
				.rta_type(Rta::Gateway)
				.rta_payload(bytes)
				.build()
				.map_err(|source| {
					tg::error!(!source, "failed to build the rta_gateway attribute")
				})?,
		);
		let msg = RtmsgBuilder::default()
			.rtm_family(RtAddrFamily::Inet)
			.rtm_dst_len(0)
			.rtm_src_len(0)
			.rtm_tos(0)
			.rtm_table(RtTable::Main)
			.rtm_protocol(Rtprot::Boot)
			.rtm_scope(RtScope::Universe)
			.rtm_type(Rtn::Unicast)
			.rtattrs(attrs)
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the rtmsg"))?;
		self.send::<Rtmsg>(Rtm::Newroute, NlmF::CREATE | NlmF::EXCL | NlmF::ACK, msg)
	}

	fn send<P>(&mut self, msg_type: Rtm, flags: NlmF, payload: P) -> tg::Result<()>
	where
		P: neli::ToBytes
			+ neli::FromBytesWithInput<Input = usize>
			+ neli::Size
			+ std::fmt::Debug
			+ Send
			+ Sync
			+ 'static,
	{
		let recv = self
			.rtnl
			.send::<_, _, Rtm, P>(msg_type, flags, NlPayload::Payload(payload))
			.map_err(|source| {
				tg::error!(!source, ?msg_type, "failed to send the netlink request")
			})?;
		for response in recv {
			response.map_err(|source| tg::error!(!source, ?msg_type, "netlink request failed"))?;
		}
		Ok(())
	}
}

fn index_of(name: &str) -> tg::Result<i32> {
	let c = CString::new(name)
		.map_err(|source| tg::error!(!source, %name, "invalid interface name"))?;
	let index = unsafe { libc::if_nametoindex(c.as_ptr()) };
	if index == 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, %name, "failed to look up the interface index"));
	}
	i32::try_from(index).map_err(|source| tg::error!(!source, "interface index overflow"))
}

fn rtattr_ifname(name: &str) -> tg::Result<Rtattr<Ifla, Buffer>> {
	RtattrBuilder::default()
		.rta_type(Ifla::Ifname)
		.rta_payload(format!("{name}\0"))
		.build()
		.map_err(|source| tg::error!(!source, "failed to build the ifname attribute"))
}

fn rtattr_linkinfo_kind(kind: &str) -> tg::Result<Rtattr<Ifla, Buffer>> {
	let info_kind: Rtattr<IflaInfo, Buffer> = RtattrBuilder::default()
		.rta_type(IflaInfo::Kind)
		.rta_payload(format!("{kind}\0"))
		.build()
		.map_err(|source| tg::error!(!source, "failed to build the info_kind attribute"))?;
	RtattrBuilder::default()
		.rta_type(Ifla::Linkinfo)
		.rta_payload(Vec::<u8>::new())
		.build()
		.map_err(|source| tg::error!(!source, "failed to build the linkinfo attribute"))?
		.nest(&info_kind)
		.map_err(|source| tg::error!(!source, "failed to nest the info_kind"))
}
