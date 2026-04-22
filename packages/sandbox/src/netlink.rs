use {
	neli::{
		consts::{
			nl::NlmF,
			rtnl::{Ifa, Iff, Ifla, RtAddrFamily, RtScope, RtTable, Rta, Rtm, Rtn, Rtprot},
			socket::NlFamily,
		},
		nl::NlPayload,
		router::synchronous::NlRouter,
		rtnl::{
			Ifaddrmsg, IfaddrmsgBuilder, Ifinfomsg, IfinfomsgBuilder, RtattrBuilder, Rtmsg,
			RtmsgBuilder,
		},
		types::{Buffer, RtBuffer},
		utils::Groups,
	},
	std::{ffi::CString, net::Ipv4Addr},
	tangram_client as tg,
};

pub struct Netlink {
	rtnl: NlRouter,
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

	pub fn addr_add_v4(&mut self, name: &str, addr: Ipv4Addr, prefix_len: u8) -> tg::Result<()> {
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
		self.send::<Ifaddrmsg>(Rtm::Newaddr, NlmF::CREATE | NlmF::EXCL | NlmF::ACK, msg)
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
