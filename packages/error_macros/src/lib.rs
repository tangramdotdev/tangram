use quote::{quote, TokenStreamExt};
use syn::{parse_macro_input, punctuated::Punctuated, Expr, Ident, Token};

struct Error {
	values: Punctuated<Value, Token![,]>,
	stack: Option<Expr>,
	source: Option<Expr>,
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

/// Generate a tangram_error::Error.
///
/// Usage:
/// ```rust
///
/// // Basic errors
/// error!("This is an error with a message.");
///
/// // String interpolation
/// error!("This is an error that uses {}.", "string interpolation");
///
/// // Errors can carry values, that are pretty-printed or debug-printed.
/// let name = "value";
/// error!(%name, "This is an error with a pretty-printed value associated with it.");
/// error!(?name, "This is an error with a debug-printed value associated with it.");
///
/// // Errors can nest.
/// let other_error = std::io::Error::other("An i/o error occurred deeper.");
/// error!(source = other_error, "This is an error that wraps another error.");
///
/// // Errors can have a stack trace.
/// let call_stack = vec![
/// 	tangram_error::Location {
/// 		source: "another_file.rs".to_owned(),
/// 		line: 123,
/// 		column: 456
/// 	}
/// ];
/// error!(stack = call_stack, "This is an error that has a stack trace associated with it.");
/// ```
#[proc_macro]
pub fn error(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let Error {
		values,
		rest,
		source,
		stack,
	} = parse_macro_input!(input as Error);

	let stack = stack
		.map(|stack| quote!(Some(#stack)))
		.unwrap_or(quote!(None));

	let source = source
		.map(|source| {
			quote! {
				Some({
					let source: Box<dyn std::error::Error + Send + Sync> = Box::new(#source);
					std::sync::Arc::new(source.into())
				})
			}
		})
		.unwrap_or(quote!(None));

	let tokens = quote! {
		{{
			tangram_error::Error {
				message: format!(#rest),
				location: Some(tangram_error::Location {
					source: file!().into(),
					line: line!() - 1,
					column: column!() - 1,
				}),
				stack: #stack,
				source: #source,
				values: [#values].into_iter().collect(),
			}
		}}
	};
	tokens.into()
}

impl syn::parse::Parse for Error {
	fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
		// Parse leading values and source = <expr>.
		let mut values = Vec::new();
		let mut source = None;
		let mut stack = None;
		loop {
			if input.peek(Ident) {
				let ident: Ident = input.parse()?;
				match ident.to_string().as_ref() {
					"source" => {
						let _: Token![=] = input.parse()?;
						source.replace(input.parse()?);
					},
					"stack" => {
						let _: Token![=] = input.parse()?;
						stack.replace(input.parse()?);
					},
					_ => return Err(input.error("Unexpected identifier.")),
				}
				let _: Token![,] = input.parse()?;
			}
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
		Ok(Self {
			values,
			stack,
			source,
			rest,
		})
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
		let stream = match formatter {
			Formatter::Debug => quote!(stringify!(#ident).into(), format!("{:?}", #ident)),
			Formatter::Display => quote!(stringify!(#ident).into(), format!("{}", #ident)),
		};
		let token_tree = proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, stream);
		tokens.append(token_tree)
	}
}
