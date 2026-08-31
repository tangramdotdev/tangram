use {
	super::{CapacityConfig, Store},
	crate::capacity::Capacity,
	futures::future,
	num::ToPrimitive as _,
	serde::Deserialize,
	std::time::{Duration, Instant},
	tangram_client::prelude::*,
	tokio::sync::Mutex,
};

pub(super) struct Client {
	available_query: String,
	cache: Mutex<Option<CacheEntry>>,
	http: reqwest::Client,
	total_query: String,
	ttl: Duration,
	url: reqwest::Url,
}

struct CacheEntry {
	cached_at: Instant,
	result: tg::Result<Capacity>,
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
		if config.ttl.is_zero() {
			return Err(tg::error!(
				"the Prometheus capacity TTL must be greater than zero"
			));
		}
		let url = config.url.trim_end_matches('/');
		let url = format!("{url}/api/v1/query")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the Prometheus URL"))?;
		let available_query = config.available_query.clone();
		let cache = Mutex::new(None);
		let total_query = config.total_query.clone();
		let http = reqwest::Client::new();
		let ttl = config.ttl;

		Ok(Self {
			available_query,
			cache,
			http,
			total_query,
			ttl,
			url,
		})
	}

	pub async fn get(&self) -> tg::Result<Capacity> {
		let mut cache = self.cache.lock().await;
		if let Some(entry) = cache.as_ref()
			&& entry.cached_at.elapsed() < self.ttl
		{
			return entry.result.clone();
		}
		let result = self.get_inner().await;
		let entry = CacheEntry {
			cached_at: Instant::now(),
			result: result.clone(),
		};
		*cache = Some(entry);

		result
	}

	async fn get_inner(&self) -> tg::Result<Capacity> {
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
	use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

	#[tokio::test]
	async fn caches_capacity_results_until_the_ttl_expires() {
		let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
			.await
			.unwrap();
		let address = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			for _ in 0..2 {
				let (mut stream, _) = listener.accept().await.unwrap();
				let mut request = [0; 4096];
				let count = stream.read(&mut request).await.unwrap();
				assert!(count > 0);
				let body = r#"{"status":"success","data":{"result":[{"value":[0,"100"]}]}}"#;
				let response = format!(
					"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
					body.len()
				);
				stream.write_all(response.as_bytes()).await.unwrap();
			}
		});
		let config = CapacityConfig {
			available_query: "sum(available)".into(),
			total_query: "sum(total)".into(),
			ttl: Duration::from_secs(1),
			url: format!("http://{address}"),
		};
		let client = Client::new(&config).unwrap();

		let first = client.get().await.unwrap();
		let second = client.get().await.unwrap();

		assert_eq!(
			first,
			Capacity {
				available: 100,
				total: 100
			}
		);
		assert_eq!(second, first);
		server.await.unwrap();
	}

	#[test]
	fn uses_the_configured_prometheus_queries() {
		let config = CapacityConfig {
			available_query: "sum(available)".into(),
			total_query: "sum(total)".into(),
			ttl: Duration::from_secs(1),
			url: "http://prometheus:9090/".into(),
		};
		let client = Client::new(&config).unwrap();

		assert_eq!(client.available_query, "sum(available)");
		assert_eq!(client.total_query, "sum(total)");
		assert_eq!(client.ttl, Duration::from_secs(1));
		assert_eq!(client.url.as_str(), "http://prometheus:9090/api/v1/query");
	}
}
