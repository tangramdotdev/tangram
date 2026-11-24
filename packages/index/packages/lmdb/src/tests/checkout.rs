use {super::super::Index, tangram_client::prelude::*};

fn put_checkout_arg(
	id: &tg::artifact::Id,
	put: [u8; 16],
	touched_at: i64,
) -> tangram_index::checkout::put::Arg {
	tangram_index::checkout::put::Arg {
		dependencies: Vec::new(),
		id: id.clone().into(),
		put,
		replace: false,
		touched_at,
	}
}

async fn put_checkout(index: &Index, arg: tangram_index::checkout::put::Arg) {
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutCheckout(Box::new(arg))],
	};
	index.write(arg).await.unwrap().unwrap();
}

#[tokio::test]
async fn put_keeps_the_larger_token_in_every_order() {
	let id: tg::artifact::Id = tg::file::Id::new(b"file").into();
	let args = [
		put_checkout_arg(&id, [1; 16], 20),
		put_checkout_arg(&id, [2; 16], 10),
	];
	for order in [[0, 1], [1, 0]] {
		let (_dir, index) = super::new_index();
		for position in order {
			put_checkout(&index, args[position].clone()).await;
		}

		// Check that the record keeps the larger token and the larger touch time.
		let checkout = index
			.try_get_checkouts(&[id.clone().into()])
			.await
			.unwrap()
			.pop()
			.unwrap()
			.unwrap();
		assert_eq!(checkout.put, [2; 16]);
		assert_eq!(checkout.touched_at, 20);

		// Check that the clean output carries the larger token.
		let output = index
			.clean(tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: 20,
				max_process_touched_at: 0,
				max_sandbox_touched_at: 0,
				now: 20,
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert_eq!(output.checkouts.len(), 1);
		assert_eq!(output.checkouts[0].id, tg::Id::from(id.clone()));
		assert_eq!(output.checkouts[0].put, [2; 16]);
	}
}

#[tokio::test]
async fn touch_keeps_the_token() {
	let (_dir, index) = super::new_index();
	let id: tg::artifact::Id = tg::file::Id::new(b"file").into();
	put_checkout(&index, put_checkout_arg(&id, [3; 16], 10)).await;
	let checkout = index
		.touch_checkouts(&[id.clone().into()], 30, std::time::Duration::ZERO)
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert_eq!(checkout.put, [3; 16]);
	assert_eq!(checkout.touched_at, 30);
}
