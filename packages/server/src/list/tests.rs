use super::*;

#[test]
fn sort_and_truncate_applies_position() {
	let entries = vec![entry("c"), entry("a"), entry("b")];
	let entries = sort_and_truncate(entries, false, Some(1), Some(1));
	let specifiers = entries
		.into_iter()
		.map(|entry| entry.specifier().to_string())
		.collect::<Vec<_>>();

	assert_eq!(specifiers, ["b"]);
}

#[test]
fn sort_and_truncate_defaults_position_to_zero() {
	let entries = vec![entry("b"), entry("a")];
	let entries = sort_and_truncate(entries, false, None, None);
	let specifiers = entries
		.into_iter()
		.map(|entry| entry.specifier().to_string())
		.collect::<Vec<_>>();

	assert_eq!(specifiers, ["a", "b"]);
}

fn entry(specifier: &str) -> tg::list::Entry {
	let id = tg::tag::Id::new();
	let name = specifier.to_owned();
	let specifier = specifier.parse().unwrap();
	let target = tg::Either::Left(tg::file::Id::new(name.as_bytes()).into());
	let target = tg::Referent::with_node(target);
	tg::list::Entry::Tag {
		id,
		location: None,
		name,
		parent: None,
		specifier,
		target,
		tokens: tg::authorization::Tokens::default(),
	}
}
