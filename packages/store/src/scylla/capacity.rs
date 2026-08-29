use {
	super::{CapacityConfig, Store},
	crate::capacity::Capacity,
	futures::future,
	num::ToPrimitive as _,
	serde::Deserialize,
	tangram_client::prelude::*,
};

pub(super) struct Client {
	available_query: String,
	http: reqwest::Client,
	total_query: String,
	url: reqwest::Url,
}

#[derive(Deserialize)]
struct Response {
	data: Data,
	error: Option<String>,
	status: String,
}

#[derive(Deserialize)]
struct Data {
	result: Vec<Sample>,
}

#[derive(Deserialize)]
struct Sample {
	value: (f64, String),
}

impl Client {
	pub fn new(config: &CapacityConfig) -> tg::Result<Self> {
		if config.available_query.trim().is_empty() {
			return Err(tg::error!(
				"the Prometheus available query must not be empty"
			));
		}
		if config.total_query.trim().is_empty() {
			return Err(tg::error!("the Prometheus total query must not be empty"));
		}
		let url = config.url.trim_end_matches('/');
		let url = format!("{url}/api/v1/query")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the Prometheus URL"))?;
		let available_query = config.available_query.clone();
		let total_query = config.total_query.clone();
		let http = reqwest::Client::new();

		Ok(Self {
			available_query,
			http,
			total_query,
			url,
		})
	}

	pub async fn get(&self) -> tg::Result<Capacity> {
		let (available, total) = future::try_join(
			self.query(&self.available_query),
			self.query(&self.total_query),
		)
		.await?;
		let capacity = Capacity { available, total };

		Ok(capacity)
	}

	async fn query(&self, query: &str) -> tg::Result<u64> {
		let mut url = self.url.clone();
		url.query_pairs_mut().append_pair("query", query);
		let response = self
			.http
			.get(url)
			.send()
			.await
			.map_err(|error| tg::error!(!error, "failed to query Prometheus"))?
			.error_for_status()
			.map_err(|error| tg::error!(!error, "Prometheus returned an error status"))?
			.json::<Response>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the Prometheus response"))?;
		if response.status != "success" {
			let error = response.error.unwrap_or_default();
			return Err(tg::error!(%error, "the Prometheus query failed"));
		}
		let [sample] = response.data.result.as_slice() else {
			return Err(tg::error!(
				"the Prometheus query did not return exactly one sample"
			));
		};
		let value = sample
			.value
			.1
			.parse::<f64>()
			.map_err(|error| tg::error!(!error, "failed to parse the Prometheus sample"))?
			.to_u64()
			.ok_or_else(|| tg::error!("the Prometheus sample was not a u64"))?;

		Ok(value)
	}
}

impl Store {
	pub async fn try_get_capacity(&self) -> tg::Result<Option<Capacity>> {
		let Some(client) = &self.capacity else {
			return Ok(None);
		};
		let capacity = client.get().await?;

		Ok(Some(capacity))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn uses_the_configured_prometheus_queries() {
		let config = CapacityConfig {
			available_query: "sum(available)".into(),
			total_query: "sum(total)".into(),
			url: "http://prometheus:9090/".into(),
		};
		let client = Client::new(&config).unwrap();

		assert_eq!(client.available_query, "sum(available)");
		assert_eq!(client.total_query, "sum(total)");
		assert_eq!(client.url.as_str(), "http://prometheus:9090/api/v1/query");
	}
}
