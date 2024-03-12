use quote::{quote, TokenStreamExt};
use syn::{parse_macro_input, punctuated::Punctuated, Expr, Ident, Token};

struct Error {
	values: Punctuated<Value, Token![,]>,
	rest: Punctuated<Expr, Token![,]>,
}

struct Value {
	formatter: Formatter,
	ident: Ident,
}

enum Formatter {
	Debug,
	Display,
}

#[proc_macro]
pub fn error(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	// let input2: proc_macro2::TokenStream = input.clone().into();
	let Error { values, rest } = parse_macro_input!(input as Error);
	let tokens = quote! {
		{{
			tangram_error::Error {
				message: format!(#rest),
				location: Some(tangram_error::Location {
					source: file!().into(),
					line: line!(),
					column: column!(),
				}),
				stack: None,
				source: None,
				values: [#values].into_iter().collect(),
			}
		}}
	};
	tokens.into()
}

impl syn::parse::Parse for Error {
	fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
		// Parse leading values.
		let mut values = Vec::new();
		loop {
			if input.peek(Token![?]) {
				let _: Token![?] = input.parse()?;
				let ident = input.parse()?;
				let formatter = Formatter::Debug;
				values.push(Value { formatter, ident });
				if input.is_empty() {
					break;
				}
				input.parse::<Token![,]>()?;
			} else if input.peek(Token![%]) {
				let _: Token![%] = input.parse()?;
				let ident = input.parse()?;
				let formatter = Formatter::Display;
				values.push(Value { formatter, ident });
				if input.is_empty() {
					break;
				}
				input.parse::<Token![,]>()?;
			} else {
				break;
			}
		}
		let values = values.into_iter().collect();
		let rest = input.parse_terminated(<Expr as syn::parse::Parse>::parse, Token![,])?;
		Ok(Self { values, rest })
	}
}

impl syn::parse::Parse for Value {
	fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
		if input.peek(Token![?]) {
			let _: Token![?] = input.parse().unwrap();
			let formatter = Formatter::Debug;
			let ident = input.parse()?;
			return Ok(Self { formatter, ident });
		}
		if input.peek(Token![%]) {
			let _: Token![%] = input.parse().unwrap();
			let formatter = Formatter::Display;
			let ident = input.parse()?;
			return Ok(Self { formatter, ident });
		}
		Err(input.error("Expected a ? or %."))
	}
}

impl quote::ToTokens for Value {
	fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
		let Self { formatter, ident } = self;
		let name = ident.to_string();
		let stream = match formatter {
			Formatter::Debug => quote!(#name.into(), format!("{:?}", #ident)),
			Formatter::Display => quote!(#name.into(), format!("{}", #ident)),
		};
		let token_tree = proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, stream);
		tokens.append(token_tree)
	}
}
