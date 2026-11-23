//! Run with DSA_TEST_DATABASE set to a disposable PostgreSQL 17 database.

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 17 database"]
async fn postgres_17_connection() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = std::env::var("DSA_TEST_DATABASE")?;
    let (client, connection) =
        tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await?;
    let connection = tokio::spawn(connection);
    let row = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await?;
    let version: i32 = row.get(0);
    assert!(
        (170000..180000).contains(&version),
        "PostgreSQL 17 required"
    );
    drop(client);
    connection.await??;
    Ok(())
}
