use {
	crate::prelude::*,
	std::collections::BTreeMap,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	pub module: tg::module::Data,
}

/// The documentation for a module.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	/// The documented declarations, which are referred to by their indices.
	pub declarations: Vec<Declaration>,

	/// The indices of the declarations of each export.
	pub exports: BTreeMap<String, Vec<usize>>,

	/// The modules, which locations refer to by their indices.
	pub modules: Vec<tg::module::Data>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Declaration {
	Class(Class),
	Enum(Enum),
	Function(Function),
	Interface(Interface),
	Namespace(Namespace),
	TypeAlias(TypeAlias),
	Variable(Variable),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Class {
	#[serde(default, rename = "abstract", skip_serializing_if = "is_false")]
	pub abstract_: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub constructors: Vec<Signature>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub extends: Vec<Type>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub implements: Vec<Type>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub name: String,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub properties: Vec<Property>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub type_parameters: Vec<TypeParameter>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Comment {
	/// The text of the deprecated tag, which is empty if the tag has no text.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub deprecated: Option<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub examples: Vec<String>,

	/// The tags that are not represented by other fields.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub tags: Vec<Tag>,

	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub text: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Tag {
	pub name: String,

	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub text: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Enum {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub members: Vec<EnumMember>,

	pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct EnumMember {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub initializer: Option<String>,

	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub value: Option<Literal>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Function {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub name: String,

	pub signature: Signature,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Interface {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub extends: Vec<Type>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	#[serde(flatten)]
	pub members: Members,

	pub name: String,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub type_parameters: Vec<TypeParameter>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Members {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub call_signatures: Vec<Signature>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub construct_signatures: Vec<Signature>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub index_signatures: Vec<IndexSignature>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub properties: Vec<Property>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Property {
	#[serde(default, rename = "abstract", skip_serializing_if = "is_false")]
	pub abstract_: bool,

	/// Whether the property is a class method.
	#[serde(default, rename = "classMethod", skip_serializing_if = "is_false")]
	pub class_method: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	/// Whether the property is a method rather than a property with a function type.
	#[serde(default, skip_serializing_if = "is_false")]
	pub method: bool,

	pub name: String,

	#[serde(default, skip_serializing_if = "is_false")]
	pub optional: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub protected: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub readonly: bool,

	#[serde(default, rename = "static", skip_serializing_if = "is_false")]
	pub static_: bool,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct IndexSignature {
	pub key: Type,

	pub name: String,

	#[serde(default, skip_serializing_if = "is_false")]
	pub readonly: bool,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Namespace {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	/// The indices of the declarations of each export.
	pub exports: BTreeMap<String, Vec<usize>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeAlias {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub name: String,

	#[serde(rename = "type")]
	pub type_: Type,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub type_parameters: Vec<TypeParameter>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Variable {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub constant: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	pub name: String,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Location {
	/// The index of the module.
	pub module: usize,

	/// The range, of the form `[start line, start character, end line, end character]`.
	pub range: [u32; 4],
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Signature {
	#[serde(default, rename = "async", skip_serializing_if = "is_false")]
	pub async_: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<Comment>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<Location>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub parameters: Vec<Parameter>,

	#[serde(rename = "return")]
	pub return_: Type,

	/// The text of the returns tag.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub returns: Option<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub type_parameters: Vec<TypeParameter>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Parameter {
	/// The text of the parameter's param tag.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,

	/// The source text of the parameter's default value.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default: Option<String>,

	/// The kind of the parameter, which is absent for a parameter that may be passed by position or by keyword.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<ParameterKind>,

	pub name: String,

	#[serde(default, skip_serializing_if = "is_false")]
	pub optional: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub rest: bool,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterKind {
	/// A parameter that may only be passed by keyword.
	KeywordOnly,

	/// A parameter that collects the remaining keyword arguments.
	KeywordRest,

	/// A parameter that may only be passed by position.
	PositionalOnly,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TypeParameter {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub constraint: Option<Type>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default: Option<Type>,

	pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Type {
	Array(Box<ArrayType>),

	Conditional(Box<ConditionalType>),

	Function(FunctionType),

	IndexedAccess(Box<IndexedAccessType>),

	Infer(Box<TypeParameter>),

	Intersection(Types),

	Keyword(String),

	Literal(Literal),

	Mapped(Box<MappedType>),

	Object(Members),

	#[serde(rename = "type_operator")]
	Operator(Box<OperatorType>),

	/// The printed text of a type that is not otherwise represented.
	Other(String),

	Predicate(Box<PredicateType>),

	#[serde(rename = "type_query")]
	Query(QueryType),

	Reference(ReferenceType),

	TemplateLiteral(TemplateLiteralType),

	Tuple(TupleType),

	Union(Types),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ArrayType {
	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionalType {
	pub check_type: Type,
	pub extends_type: Type,
	pub false_type: Type,
	pub true_type: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FunctionType {
	pub signatures: Vec<Signature>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexedAccessType {
	pub index_type: Type,
	pub object_type: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Literal {
	Bigint(String),
	Boolean(bool),
	Null,
	Number(serde_json::Number),
	String(String),
	Undefined,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MappedType {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub constraint: Option<Type>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name_type: Option<Type>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub optional: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub readonly: bool,

	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<Type>,

	pub type_parameter_name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OperatorType {
	pub operator: String,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PredicateType {
	#[serde(default, skip_serializing_if = "is_false")]
	pub asserts: bool,

	pub name: String,

	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<Type>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryType {
	pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceType {
	/// The indices of the referenced declarations, which is empty if the referenced symbol is not documented.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub declarations: Vec<usize>,

	/// The fully qualified name, if it differs from the name.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fully_qualified_name: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub is_type_parameter: bool,

	pub name: String,

	/// Whether the referenced symbol is declared by the runtime.
	#[serde(default, skip_serializing_if = "is_false")]
	pub runtime: bool,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub type_arguments: Vec<Type>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateLiteralType {
	pub head: String,
	pub template_spans: Vec<TemplateLiteralTypeSpan>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TemplateLiteralTypeSpan {
	pub literal: String,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TupleType {
	pub elements: Vec<TupleElement>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TupleElement {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub optional: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub rest: bool,

	#[serde(rename = "type")]
	pub type_: Type,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Types {
	pub types: Vec<Type>,
}

impl tg::Session {
	pub async fn document(&self, arg: Arg) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = "/document";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}
