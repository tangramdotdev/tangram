use {
	crate::Result,
	serde::{Deserialize, Serialize},
};

#[cfg(test)]
mod tests;

#[derive(
	Clone,
	Copy,
	Debug,
	Deserialize,
	Eq,
	PartialEq,
	Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Limits {
	#[tangram_serialize(id = 0)]
	pub bytes: u64,

	#[tangram_serialize(id = 1)]
	pub messages: u64,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Deserialize,
	Eq,
	PartialEq,
	Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Consumption {
	#[tangram_serialize(id = 0)]
	pub bytes: u64,

	#[tangram_serialize(id = 1)]
	pub messages: u64,
}

pub struct Sender {
	consumption: Consumption,
	limits: Limits,
	sent: Consumption,
}

pub struct Receiver {
	consumption: Consumption,
	limits: Limits,
	received: Consumption,
	reported: Consumption,
}

impl Limits {
	pub fn validate(self) -> Result<()> {
		if self.bytes == 0
			|| self.messages == 0
			|| self.messages > ((tokio::sync::Semaphore::MAX_PERMITS - 4) / 2) as u64
		{
			return Err(std::io::Error::other("invalid flow limits").into());
		}
		Ok(())
	}
}

impl Sender {
	#[must_use]
	pub fn new(limits: Limits) -> Self {
		Self {
			consumption: Consumption::default(),
			limits,
			sent: Consumption::default(),
		}
	}

	#[must_use]
	pub fn available(&self, bytes: usize) -> bool {
		bytes as u64
			<= self
				.limits
				.bytes
				.saturating_sub(self.sent.bytes - self.consumption.bytes)
			&& self.sent.messages - self.consumption.messages < self.limits.messages
	}

	pub fn send(&mut self, bytes: usize) -> Result<()> {
		if !self.available(bytes) {
			return Err(std::io::Error::other("the flow window was exceeded").into());
		}
		self.sent = add(self.sent, bytes)?;
		Ok(())
	}

	pub fn update(&mut self, consumption: Consumption) -> Result<()> {
		if consumption.bytes < self.consumption.bytes
			|| consumption.messages < self.consumption.messages
			|| consumption.bytes > self.sent.bytes
			|| consumption.messages > self.sent.messages
		{
			return Err(std::io::Error::other("invalid flow consumption").into());
		}
		self.consumption = consumption;
		Ok(())
	}
}

impl Receiver {
	#[must_use]
	pub fn new(limits: Limits) -> Self {
		Self {
			consumption: Consumption::default(),
			limits,
			received: Consumption::default(),
			reported: Consumption::default(),
		}
	}

	pub fn receive(&mut self, bytes: usize) -> Result<()> {
		let received = add(self.received, bytes)?;
		if received.bytes - self.consumption.bytes > self.limits.bytes
			|| received.messages - self.consumption.messages > self.limits.messages
		{
			return Err(std::io::Error::other("the flow window was exceeded").into());
		}
		self.received = received;
		Ok(())
	}

	pub fn consume(&mut self, bytes: usize) -> Result<Option<Consumption>> {
		let consumption = add(self.consumption, bytes)?;
		if consumption.bytes > self.received.bytes || consumption.messages > self.received.messages
		{
			return Err(std::io::Error::other("invalid flow consumption").into());
		}
		self.consumption = consumption;
		if self.consumption.bytes - self.reported.bytes < self.limits.bytes.div_ceil(2)
			&& self.consumption.messages - self.reported.messages < self.limits.messages.div_ceil(2)
		{
			return Ok(None);
		}
		Ok(self.flush())
	}

	pub fn flush(&mut self) -> Option<Consumption> {
		if self.consumption == self.reported {
			return None;
		}
		self.reported = self.consumption;
		Some(self.consumption)
	}
}

fn add(value: Consumption, bytes: usize) -> Result<Consumption> {
	let bytes = value
		.bytes
		.checked_add(bytes as u64)
		.ok_or_else(|| std::io::Error::other("the flow byte count overflowed"))?;
	let messages = value
		.messages
		.checked_add(1)
		.ok_or_else(|| std::io::Error::other("the flow message count overflowed"))?;
	Ok(Consumption { bytes, messages })
}
