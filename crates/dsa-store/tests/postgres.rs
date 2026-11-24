//! Run with DSA_TEST_DATABASE set to a disposable PostgreSQL 17 or newer
//! database.

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 17 or newer database"]
async fn postgres_server_is_supported() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = std::env::var("DSA_TEST_DATABASE")?;
    let (client, connection) =
        tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await?;
    let connection = tokio::spawn(connection);
    let row = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await?;
    let version: i32 = row.get(0);
    dsa_store::supported_server(version)?;
    // CI names the major version each of its jobs runs on.
    let expected = std::env::var("DSA_TEST_SERVER_MAJOR").unwrap_or_default();
    if !expected.is_empty() {
        assert_eq!(
            (version / 10000).to_string(),
            expected,
            "the server's major version"
        );
    }
    drop(client);
    connection.await??;
    Ok(())
}
