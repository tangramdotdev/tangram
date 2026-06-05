pub(crate) fn tangram() -> syntect::highlighting::Theme {
	let foreground = color(0xdd, 0xdd, 0xdd);
	let background = color(0x00, 0x00, 0x00);
	let red = color(0xff, 0x44, 0x3a);
	let green = color(0x30, 0xd1, 0x58);
	let yellow = color(0xff, 0xd6, 0x0a);
	let blue = color(0x0a, 0x84, 0xff);
	let magenta = color(0xbf, 0x5a, 0xf2);
	let cyan = color(0x64, 0xd3, 0xff);
	let comment = color(0x77, 0x77, 0x77);
	let punctuation = color(0xbb, 0xbb, 0xbb);
	let selection = color(0x33, 0x33, 0x33);

	syntect::highlighting::Theme {
		name: Some("tangram".to_owned()),
		author: Some("Tangram".to_owned()),
		settings: syntect::highlighting::ThemeSettings {
			foreground: Some(foreground),
			background: Some(background),
			caret: Some(foreground),
			accent: Some(cyan),
			selection: Some(selection),
			selection_foreground: Some(foreground),
			..Default::default()
		},
		scopes: vec![
			item(
				"comment",
				comment,
				Some(syntect::highlighting::FontStyle::ITALIC),
			),
			item("punctuation", punctuation, None),
			item("string", green, None),
			item("constant.character.escape", cyan, None),
			item("constant.numeric", yellow, None),
			item("constant.language.boolean", yellow, None),
			item(
				"constant.language.null,variable.language.undefined",
				comment,
				None,
			),
			item("constant,constant.other", red, None),
			item("keyword,storage", magenta, None),
			item(
				"entity.name.function,support.function,variable.function",
				yellow,
				None,
			),
			item(
				"entity.name.function.macro,support.function.macro",
				red,
				None,
			),
			item(
				"entity.name.type,entity.name.class,support.type,support.class",
				blue,
				None,
			),
			item("entity.name.namespace,entity.name.tag", blue, None),
			item(
				"support.type.builtin,support.class.builtin,entity.name.tag.builtin",
				cyan,
				None,
			),
			item("variable", cyan, None),
			item("variable.language", magenta, None),
			item(
				"meta.object-literal.key,variable.other.property,support.variable.property",
				green,
				None,
			),
			item(
				"invalid,invalid.illegal",
				red,
				Some(syntect::highlighting::FontStyle::UNDERLINE),
			),
		],
	}
}

fn item(
	scope: &str,
	foreground: syntect::highlighting::Color,
	font_style: Option<syntect::highlighting::FontStyle>,
) -> syntect::highlighting::ThemeItem {
	syntect::highlighting::ThemeItem {
		scope: scope.parse().expect("invalid syntect scope selector"),
		style: syntect::highlighting::StyleModifier {
			foreground: Some(foreground),
			background: None,
			font_style,
		},
	}
}

fn color(r: u8, g: u8, b: u8) -> syntect::highlighting::Color {
	syntect::highlighting::Color { r, g, b, a: 0xff }
}
