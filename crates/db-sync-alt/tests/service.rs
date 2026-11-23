//! The service's parts besides following: the `health` command against a
//! running HTTP server (no database), and against PostgreSQL 17 in
//! DSA_TEST_DATABASE the status document of `db-sync-alt status`, the HTTP
//! answers from a refreshed database view, index upkeep, a month of churn,
//! the daily snapshot files and the supply check.

use std::{
    net::SocketAddr,
    process::Output,
    time::{Duration, Instant},
};

use dsa_service::{http::serve, status::Documents};
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// An HTTP server of the service in this process, answering `healthy`.
async fn health_server(
    healthy: bool,
) -> (SocketAddr, watch::Sender<Documents>, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (documents, receiver) = watch::channel(Documents {
        healthy,
        body: "{}".into(),
    });
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(serve(listener, receiver, async {
        let _ = stopped.await;
    }));
    (address, documents, stop)
}

async fn health(arguments: &[&str], environment: &[(&str, &str)]) -> (Output, Duration) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    command
        .arg("health")
        .args(arguments)
        .env_remove("DSA_HEALTH_URL");
    for (name, value) in environment {
        command.env(name, value);
    }
    let started = Instant::now();
    let output = command.output().await.unwrap();
    (output, started.elapsed())
}

/// The `health` command exits 0 on 200 and 1 otherwise within 3 seconds,
/// reads `DSA_HEALTH_URL`, and the flag wins.
#[tokio::test]
async fn the_health_command_exits_0_on_200_and_1_otherwise() -> Result {
    let (address, documents, stop) = health_server(true).await;
    let url = format!("http://{address}/health");
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(took < Duration::from_secs(3));
    let printed: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(printed["http_status"], 200);
    // From the environment, and the flag wins over it.
    let (output, _) = health(&[], &[("DSA_HEALTH_URL", &url)]).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let (output, _) = health(
        &["--url", &url],
        &[("DSA_HEALTH_URL", "http://127.0.0.1:1/health")],
    )
    .await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    // Unhealthy: 503 is exit 1.
    documents.send(Documents {
        healthy: false,
        body: "{}".into(),
    })?;
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_secs(3));
    assert!(String::from_utf8_lossy(&output.stderr).contains("service_unhealthy"));
    // Nothing listens.
    let _ = stop.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_secs(3));
    // A listener that never answers.
    let silent = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/health", silent.local_addr()?);
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_millis(3500), "{took:?}");
    // An invalid address is a usage error, found before any connection.
    for invalid in ["https://127.0.0.1/health", "127.0.0.1:18080", "http://h:0/"] {
        let (output, _) = health(&["--url", invalid], &[]).await;
        assert_eq!(output.status.code(), Some(2), "{invalid}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("\"flag\":\"--url\""));
    }
    let (output, _) = health(&[], &[("DSA_HEALTH_URL", "ftp://x")]).await;
    assert_eq!(output.status.code(), Some(2));
    drop(silent);
    Ok(())
}
