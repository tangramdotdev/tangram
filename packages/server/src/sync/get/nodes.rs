use {
	crate::{
		Session,
		sync::{graph::Graph, queue::Queue},
	},
	std::sync::Mutex,
	tangram_client::prelude::*,
	tokio::sync::mpsc,
};

impl Session {
	pub(super) async fn sync_get_nodes(
		arg: &tg::sync::Arg,
		graph: &Mutex<Graph>,
		queue: &Queue,
		sender: &mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
		mut receiver: Option<mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
	) -> tg::Result<Vec<tg::Referent<tg::Selector<tg::Id>>>> {
		// Enqueue the initial nodes before receiving additional nodes.
		let mut get = arg.get.clone();
		for node in &get {
			Self::sync_get_node(arg, graph, queue, sender, node).await?;
		}
		if let Some(receiver) = &mut receiver {
			while let Some(node) = receiver.recv().await {
				Self::sync_get_node(arg, graph, queue, sender, &node).await?;
				get.push(node);
			}
		}

		// Close the queue only after all nodes have arrived and completed.
		let mut graph = graph.lock().unwrap();
		graph.set_get_open(false);
		if graph.end_local() {
			queue.close();
		}

		Ok(get)
	}

	async fn sync_get_node(
		arg: &tg::sync::Arg,
		graph: &Mutex<Graph>,
		queue: &Queue,
		sender: &mpsc::Sender<tg::Result<tg::sync::GetMessage>>,
		node: &tg::Referent<tg::Selector<tg::Id>>,
	) -> tg::Result<()> {
		let tokens = node.options.tokens.clone();
		match &node.node {
			tg::Selector::Id(id) => {
				let local_tokens = tokens.local_entry();
				let remote_tokens = tokens.remote_entry();
				{
					let mut graph = graph.lock().unwrap();
					graph.insert_local_root(id.clone());
					graph.update_root_tokens(id, &local_tokens, &remote_tokens);
				}
				queue.enqueue(arg.eager, id.clone(), local_tokens, remote_tokens)?;
			},
			tg::Selector::Specifier(specifier) => {
				graph
					.lock()
					.unwrap()
					.insert_local_selector(specifier.clone());
				let message = tg::sync::GetNodeMessage {
					descendants: true,
					eager: arg.eager,
					selector: tg::Selector::Specifier(specifier.clone()),
					tokens,
				};
				sender
					.send(Ok(tg::sync::GetMessage::Node(message)))
					.await
					.map_err(|error| tg::error!(!error, "failed to send the message"))?;
			},
		}
		Ok(())
	}
}
