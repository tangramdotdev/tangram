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

/// Check that the checkout record keeps the larger token and touch time when the puts are applied in the given order.
async fn check_put_keeps_the_larger_token(index: &Index, order: [usize; 2]) {
	let id: tg::artifact::Id = tg::file::Id::new(b"file").into();
	let args = [
		put_checkout_arg(&id, [1; 16], 20),
		put_checkout_arg(&id, [2; 16], 10),
	];
	for position in order {
		put_checkout(index, args[position].clone()).await;
	}
	let checkout = index
		.try_get_checkouts(&[id.clone().into()])
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert_eq!(checkout.put, [2; 16]);
	assert_eq!(checkout.touched_at, 20);
}

#[tokio::test]
async fn put_keeps_the_larger_token_in_order() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		check_put_keeps_the_larger_token(index, [0, 1]).await;
	})
	.await;
}

#[tokio::test]
async fn put_keeps_the_larger_token_in_reverse_order() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		check_put_keeps_the_larger_token(index, [1, 0]).await;
	})
	.await;
}
