use insta::assert_snapshot;
use serde_json::json;

mod common;
use common::test;

#[tokio::test]
async fn test_health() -> std::io::Result<()> {
	test(
		json!({
			"advanced": {
				"file_descriptor_semaphore_size": 4096
			},
			"database": {
				"kind": "sqlite",
				"connections": 1
			},
		}),
		&["health"],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @r###"
   {
     "builds": {
       "created": 0,
       "dequeued": 0,
       "started": 0
     },
     "database": {
       "available_connections": 2
     },
     "file_descriptor_semaphore": {
       "available_permits": 4096
     },
     "version": null
   }
   "###);
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}
