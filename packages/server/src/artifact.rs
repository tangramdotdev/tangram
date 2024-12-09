pub mod cache;
mod checkin;
mod checkout;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct Progress {
	objects: u64,
	bytes: u64,
}
