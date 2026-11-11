use super::is_repeatable;

#[test]
fn only_reads_are_repeated_after_a_failure_before_the_response() {
	assert!(is_repeatable(&http::Method::GET));
	assert!(is_repeatable(&http::Method::HEAD));
	assert!(!is_repeatable(&http::Method::POST));
	assert!(!is_repeatable(&http::Method::PUT));
	assert!(!is_repeatable(&http::Method::PATCH));
	assert!(!is_repeatable(&http::Method::DELETE));
}
