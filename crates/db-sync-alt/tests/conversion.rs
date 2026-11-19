//! Generated true schema5 sources with the unchanged legacy gate and reader.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::fixtures::selected;
use common::{Database, connect, failure, file_args, fixture};
use dsa_chain::{
    keep::{Header, TABLES, keep},
    profiles::Profile,
};
use dsa_store::{
    Store,
    convert::{ConvertOptions, LegacySource},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    time::{Duration, Instant},
};
use tokio_postgres::config::SslMode;
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_true_legacy_public_reader_model_and_snapshot() -> Result {
    let f = fixture(201);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let path = dir.path().join("converted.dsa");
    let source_dsn = source.dsn(true);
    let converted = target
        .cli(
            "convert",
            &f.profile.network,
            &[
                "--source",
                &source_dsn,
                "--horizon-block",
                "6",
                "--batch-rows",
                "3",
                "--batch-bytes",
                "2048",
                "--snapshot",
                path.to_str().unwrap(),
            ],
        )
        .await?;
    let expected = selected(&f.rows(), &keep(&f.chain, &f.profile, f.bounds)?);
    assert_eq!(target.rows().await?, expected);
    assert_eq!(
        converted["verified"]["state"]["target"]["height"],
        f.bounds.target
    );
    assert_eq!(
        converted["source"]["settings"]["transaction_read_only"],
        "on"
    );
    assert_eq!(
        converted["source"]["settings"]["default_transaction_read_only"],
        "on"
    );
    let dest = Database::new().await?;
    let restored = dest
        .cli("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert_eq!(
        restored["content_digest"],
        converted["verified"]["content_digest"]
    );
    assert_eq!(
        restored["state_digest"],
        converted["verified"]["state_digest"]
    );
    assert_eq!(dest.rows().await?, expected);
    let old_spent = f
        .chain
        .inputs
        .values()
        .filter(|i| f.chain.transactions[&i.transaction].block < i64::from(f.bounds.horizon))
        .map(|i| i.output)
        .collect::<BTreeSet<_>>();
    let expected_markers = expected["tx_out"]
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .filter(|id| old_spent.contains(id))
        .collect::<BTreeSet<_>>();
    let client = connect(&target.config).await?;
    let actual = client
        .query("SELECT tx_out_id FROM dsa.consumed_before_horizon", &[])
        .await?
        .into_iter()
        .map(|r| r.get::<_, i64>(0))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected_markers);
    assert!(
        !std::fs::read(&path)?
            .windows(source_dsn.len())
            .any(|b| b == source_dsn.as_bytes()),
        "snapshot disclosed source DSN"
    );
    drop(client);
    source.cleanup().await?;
    target.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_preflight_refusals_preserve_pristine_and_created_destinations() -> Result {
    let f = fixture(203);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let dir = tempfile::tempdir()?;
    for created in [false, true] {
        if created {
            target.cli("create", "mainnet", &[]).await?;
        }
        let before = target.catalog().await?;
        source
            .write("UPDATE chain.service_state SET revoked=true")
            .await?;
        target
            .refusal("mainnet", &source.dsn(true), &[], "source_not_serving")
            .await?;
        source
            .write("UPDATE chain.service_state SET revoked=false")
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(false),
                &[],
                "invalid_source_reader_role",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &["--horizon-block", "9"],
                "horizon_above_source_target",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &[
                    "--snapshot",
                    dir.path().join("missing/file.dsa").to_str().unwrap(),
                ],
                "invalid_snapshot_destination",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &target.dsn(false),
                &[],
                "invalid_source_reader_role",
            )
            .await?;
        assert_eq!(target.catalog().await?, before);
    }
    // Occupied legacy source as its own destination is refused before mutation.
    let before = source.catalog().await?;
    source
        .refusal("mainnet", &source.dsn(true), &[], "destination_not_empty")
        .await?;
    assert_eq!(source.catalog().await?, before);
    // A completed destination is also protected, including its gate and ACL.
    target
        .cli(
            "convert",
            "mainnet",
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    let before = target.catalog().await?;
    let rows = target.rows().await?;
    target
        .refusal("mainnet", &source.dsn(true), &[], "destination_not_empty")
        .await?;
    assert_eq!(target.catalog().await?, before);
    assert_eq!(target.rows().await?, rows);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_network_identity_rejects_shared_units_and_uses_positive_anchor() -> Result {
    // Preview-like schema5 data: only shared Preprod/Preview policies, never
    // placeholder multi_asset rows as evidence of a held network-specific unit.
    let mut f = fixture(204);
    let shared = f.profile.assets["cnight"].policy.clone();
    f.chain
        .output_assets
        .retain(|_, a| f.chain.assets[&a.asset].policy == shared);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &[],
            "source_network_ambiguous",
        )
        .await?;
    // Preview is deliberately unselectable before provider rebuild support.
    target
        .refusal("preview", &source.dsn(true), &[], "unsupported_network")
        .await?;
    assert_eq!(target.catalog().await?, before);
    let preview: Profile =
        serde_json::from_str(include_str!("../../dsa-chain/profiles/preview.json"))?;
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = session.snapshot().await?;
    let mut store = Store::connect(target.config.clone(), None).await?;
    let error = match store
        .stage_conversion(&preview, &tx, &ConvertOptions::default())
        .await
    {
        Ok(_) => panic!("placeholder Preview profile was accepted"),
        Err(error) => error,
    };
    assert_eq!(
        error.report()["error"],
        "source_profile_has_no_positive_anchor"
    );
    drop(tx);
    drop(session);
    drop(store);
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    // A genuine positive Preprod anchor also works before any exclusive mint.
    let mut f = fixture(206);
    let offset = f.profile.start.block_number;
    f.translate_heights(offset);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    // A covered, wrong hash must refuse even with positive exclusive assets.
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &["--horizon-block", &f.bounds.horizon.to_string()],
            "source_start_anchor_mismatch",
        )
        .await?;
    assert_eq!(target.catalog().await?, before);
    source
        .write(&format!(
            "UPDATE serve.block SET hash=decode('{}','hex') WHERE id={offset}",
            f.profile.start.block_hash
        ))
        .await?;
    let out = target
        .cli(
            "convert",
            "preprod",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &f.bounds.horizon.to_string(),
            ],
        )
        .await?;
    assert_eq!(out["source"]["network_evidence"], "pinned_start_header");
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_sparse_origins_continuous_suffix_and_missing_window_headers() -> Result {
    let mut f = fixture(208);
    let start = f.profile.start.block_number;
    f.translate_heights(start);
    // Move an ordinary still-unspent origin below the initial cursor, leaving
    // one isolated header there and the complete later header suffix intact.
    let old = f.output(4_318_350, 0, 0, "ics", "ics", None);
    f.chain.blocks.insert(
        4_318_350,
        Header {
            epoch: Some(0),
            slot: Some(1),
        },
    );
    let source = Database::new().await?;
    source.legacy(&f).await?;
    source.write(&format!("UPDATE serve.block SET hash=decode('{}','hex') WHERE id={start};UPDATE serve.block SET time=timestamp '2026-01-01'+id*interval '1second'", f.profile.start.block_hash)).await?;
    let target = Database::new().await?;
    // Short history clamps to the verified start, not to an isolated old row.
    let out = target
        .cli("convert", "preprod", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start);
    assert_eq!(out["source"]["horizon_search_start_height"], start);
    assert_eq!(out["source"]["source_first_header"], 4_318_350);
    assert_eq!(out["source"]["horizon_clamped_to_source_start"], true);
    assert!(
        target.rows().await?["tx_out"]
            .iter()
            .any(|r| r["id"] == old)
    );
    target.cleanup().await?;
    // An adjacent prefix before the known anchor is valid too. Stop at its
    // actual beginning, not at either the profile anchor or the isolated row.
    source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5(n::text)||md5('prefix/'||n::text),'hex'),0,n,n,timestamp '2026-01-01'+n*interval '1second',0 FROM generate_series({}::bigint,{}::bigint)n",start-3,start-1)).await?;
    let extended = Database::new().await?;
    let out = extended
        .cli("convert", "preprod", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start - 3);
    assert!(out["source"]["horizon_prefix_chunks"].as_u64().unwrap() > 0);
    extended.cleanup().await?;
    let explicit = Database::new().await?;
    let out = explicit
        .cli(
            "convert",
            "preprod",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &(start - 2).to_string(),
            ],
        )
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start - 2);
    explicit.cleanup().await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &["--horizon-block", "4318351"],
            "source_header_gap",
        )
        .await?;
    source
        .write(&format!("DELETE FROM serve.block WHERE id={}", start + 1))
        .await?;
    target
        .refusal("preprod", &source.dsn(true), &[], "source_header_gap")
        .await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &["--horizon-block", &(start + 1).to_string()],
            "source_header_gap",
        )
        .await?;
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_target_time_cutoff_clamp_k10_and_first_served() -> Result {
    for seed in [209, 210] {
        let mut f = fixture(seed);
        f.no_window_spend();
        // T=8 is weeks in the past relative to the test; H=1 has exact 7-day age.
        let source = Database::new().await?;
        source.legacy(&f).await?;
        source
            .write("UPDATE serve.block SET time=timestamp '2020-01-01'+id*interval '1 day'")
            .await?;
        let target = Database::new().await?;
        let out = target
            .cli(
                "convert",
                &f.profile.network,
                &["--source", &source.dsn(true), "--batch-bytes", "1024"],
            )
            .await?;
        assert_eq!(out["source"]["state"]["horizon"]["height"], 1);
        assert_eq!(out["source"]["covered_seconds"], "604800.000000");
        assert_eq!(out["source"]["coverage_shorter_than_retention"], false);
        let mut bounds = f.bounds;
        bounds.horizon = 1;
        assert_eq!(
            target.rows().await?,
            selected(&source.rows().await?, &keep(&f.chain, &f.profile, bounds)?)
        );
        let c = connect(&target.config).await?;
        let row=c.query_one("SELECT count(*),min(height)::bigint,max(height)::bigint,bool_and(first_served_at>clock_timestamp()-interval '1 minute') FROM dsa.first_served",&[]).await?;
        assert_eq!(row.get::<_, i64>(0), 8);
        assert_eq!(row.get::<_, i64>(1), 1);
        assert_eq!(row.get::<_, i64>(2), 8);
        assert!(row.get::<_, bool>(3));
        drop(c);
        source.cleanup().await?;
        target.cleanup().await?;
    }
    let mut f = fixture(211);
    f.bounds.tip += 100;
    f.bounds.target += 100;
    for h in 0..=f.bounds.tip {
        f.chain.blocks.entry(i64::from(h)).or_insert(Header {
            epoch: Some((h / 2) as i32),
            slot: Some(i64::from(h) * 100 + 1),
        });
    }
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let out = target
        .cli("convert", "mainnet", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["target"]["height"], 108);
    assert_eq!(out["source"]["state"]["horizon"]["height"], 0);
    assert_eq!(out["source"]["horizon_clamped_to_source_start"], true);
    assert_eq!(out["source"]["coverage_shorter_than_retention"], true);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_corrupt_selected_parents_are_not_silently_dropped() -> Result {
    for orphan_asset in [false, true] {
        let mut f = fixture(213);
        if !orphan_asset {
            let old = f.output(1, 9, 12, "ics", "ics", None);
            f.chain.output_assets.retain(|_, a| a.output != old);
            f.add_asset(old, 9001, 19);
            f.chain
                .transactions
                .remove(&f.chain.outputs[&old].transaction);
        }
        let source = Database::new().await?;
        source.legacy(&f).await?;
        if orphan_asset {
            let id = dsa_chain::ids::asset_id(8, 0, 13, 19)?;
            source
                .write(&format!(
                    "INSERT INTO serve.ma_tx_out VALUES({id},1,{},{})",
                    dsa_chain::ids::output_id(8, 0, 13)?,
                    f.asset_id("cnight")
                ))
                .await?;
        }
        let target = Database::new().await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &["--horizon-block", "6"],
                "referential_closure_failed",
            )
            .await?;
        target.closed_empty().await?;
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_readonly_session_counters_and_coordinator_snapshot() -> Result {
    let f = fixture(215);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    source
        .admin
        .batch_execute(&format!(
            "ALTER ROLE {} SET default_transaction_read_only=off",
            source.reader
        ))
        .await?;
    let mut reader = LegacySource::connect(source.reader_config(), None).await?;
    assert_eq!(
        reader
            .client()
            .query_one("SHOW default_transaction_read_only", &[])
            .await?
            .get::<_, String>(0),
        "on"
    );
    // It is the same actual product source connection, without private/TEMP grants.
    assert!(
        reader
            .client()
            .batch_execute("CREATE TEMP TABLE forbidden(x int)")
            .await
            .is_err()
    );
    assert!(
        reader
            .client()
            .query("SELECT * FROM chain.service_state", &[])
            .await
            .is_err()
    );
    let before:Value=source.admin.query_one("SELECT jsonb_build_array(tup_inserted,tup_updated,tup_deleted) FROM pg_stat_database WHERE datname=$1",&[&source.name]).await?.get(0);
    let mut store = Store::connect(target.config.clone(), None).await?;
    let snapshot = reader.snapshot().await?;
    let stage = store
        .stage_conversion(
            &f.profile,
            &snapshot,
            &ConvertOptions {
                horizon_block: Some(6),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        snapshot
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(stage.source().state.tip.height)
    );
    // Read counters before the independent generated follower writes.
    let after:Value=source.admin.query_one("SELECT jsonb_build_array(tup_inserted,tup_updated,tup_deleted) FROM pg_stat_database WHERE datname=$1",&[&source.name]).await?.get(0);
    assert_eq!(before, after);
    source.write(&format!("INSERT INTO serve.block VALUES({},decode('{}','hex'),999,999999,{},timestamp '2026-01-02',0)",f.bounds.tip+1,"ef".repeat(32),f.bounds.tip+1)).await?;
    assert_eq!(
        snapshot
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(f.bounds.tip)
    );
    let fresh = connect(&source.reader_config()).await?;
    assert_eq!(
        fresh
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(f.bounds.tip + 1)
    );
    snapshot.commit().await?;
    let out = stage.finish(&f.profile).await?;
    assert_eq!(out.verified.state.tip.height, f.bounds.tip);
    // An actual source write is refused by default; this attempt cannot alter
    // counters or commit a row, independently of its missing table privilege.
    let tx = reader.snapshot().await?;
    let err = tx
        .batch_execute("CREATE TABLE public.forbidden(x int)")
        .await
        .unwrap_err();
    assert_eq!(err.code().unwrap().code(), "25006");
    drop(tx);
    drop(fresh);
    drop(reader);
    drop(store);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_tls_default_refuses_plaintext_explicit_local_works() -> Result {
    let db = Database::new().await?;
    assert_eq!(
        connect(&db.config)
            .await?
            .query_one("SHOW ssl", &[])
            .await?
            .get::<_, String>(0),
        "off"
    );
    let mut implicit = db.config.clone();
    implicit.ssl_mode(SslMode::Prefer);
    assert!(Store::connect(implicit.clone(), None).await.is_err());
    assert!(LegacySource::connect(implicit, None).await.is_err());
    drop(Store::connect(db.config.clone(), None).await?);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_cancellation_and_explicit_destination_idle_timeout_retry() -> Result {
    for cancel in [true, false] {
        let f = fixture(217);
        let source = Database::new().await?;
        source.legacy(&f).await?;
        let target = Database::new().await?;
        let barrier = source.barrier().await?;
        let dsn = source.dsn(true);
        let mut child = target.start_cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &dsn,
                "--horizon-block",
                "6",
                "--idle-transaction-timeout-seconds",
                "1",
            ],
        )?;
        source.wait_barrier().await?;
        if cancel {
            child.kill().await?;
        } else {
            // The destination waits for the source sort; its explicit one-second
            // idle limit must remain effective, never silently changed to zero.
            tokio::time::sleep(Duration::from_millis(1600)).await;
        }
        barrier
            .query_one("SELECT pg_advisory_unlock(726,1)", &[])
            .await?;
        let out = child.wait_with_output().await?;
        assert!(!out.status.success());
        target.closed_empty().await?;
        let out = target
            .cli(
                "convert",
                "mainnet",
                &[
                    "--source",
                    &dsn,
                    "--horizon-block",
                    "6",
                    "--idle-transaction-timeout-seconds",
                    "120",
                ],
            )
            .await?;
        assert!(out["verified"]["content_digest"].is_string());
        drop(barrier);
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_late_snapshot_failure_reports_published_digest_and_recovery() -> Result {
    let f = fixture(219);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let root = tempfile::tempdir()?;
    let parent = root.path().join("output");
    std::fs::create_dir(&parent)?;
    let path = parent.join("snap.dsa");
    let barrier = source.barrier().await?;
    let child = target.start_cli(
        "convert",
        "mainnet",
        &[
            "--source",
            &source.dsn(true),
            "--horizon-block",
            "6",
            "--snapshot",
            path.to_str().unwrap(),
        ],
    )?;
    source.wait_barrier().await?;
    std::fs::rename(&parent, root.path().join("moved"))?;
    barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    let report = failure(&child.wait_with_output().await?)?;
    assert_eq!(report["error"], "conversion_completed_snapshot_failed");
    assert_eq!(report["converted"], true);
    let verified = target.cli("verify", "mainnet", &[]).await?;
    assert_eq!(report["content_digest"], verified["content_digest"]);
    assert_eq!(report["state_digest"], verified["state_digest"]);
    assert!(report["recovery"].as_str().unwrap().contains("snapshot"));
    std::fs::create_dir(&parent)?;
    target.cli("snapshot", "mainnet", &file_args(&path)).await?;
    drop(barrier);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_large_lossless_rows_and_k10_fallback() -> Result {
    let mut f = fixture(221);
    f.no_window_spend();
    let zero = f.asset_id("auth");
    f.chain.output_assets.retain(|_, a| a.asset != zero);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    // Use our own supported structured JSONB, never third-party reproduction.
    let nested = format!(
        "{}340282366920938463463374607431768211456{}",
        "[".repeat(200),
        "]".repeat(200)
    );
    source.write(&format!("UPDATE serve.datum SET value='{nested}'::jsonb;UPDATE serve.tx_metadata SET json=jsonb_build_object('structured','{nested}'::jsonb,'bytes',repeat('x',8192));UPDATE serve.tx_out SET value=(value::text||'.00000')::numeric")).await?;
    let target = Database::new().await?;
    let converted = target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--batch-bytes",
                "1024",
                "--batch-rows",
                "2",
            ],
        )
        .await?;
    assert_eq!(
        converted["verified"]["supply"][f.profile.assets["auth"].unit()],
        "0"
    );
    let c = connect(&target.config).await?;
    let original = connect(&source.config).await?;
    for table in ["datum", "tx_metadata", "tx_out"] {
        let rows = c
            .query(
                &format!("SELECT id,to_jsonb(r)::text FROM serve.{table} r ORDER BY id"),
                &[],
            )
            .await?;
        for row in rows {
            let text: String = original
                .query_one(
                    &format!("SELECT to_jsonb(r)::text FROM serve.{table} r WHERE id=$1"),
                    &[&row.get::<_, i64>(0)],
                )
                .await?
                .get(0);
            assert_eq!(row.get::<_, String>(1), text);
        }
    }
    let expected = keep(&f.chain, &f.profile, f.bounds)?;
    let count: i64 = c
        .query_one("SELECT count(*) FROM serve.tx_in", &[])
        .await?
        .get(0);
    assert_eq!(count as usize, expected["tx_in"].len());
    assert_eq!(count, 1);
    assert!(
        converted["source"]["families"]["datum"]["jsonb_utf8_bytes"]
            .as_u64()
            .unwrap()
            > 1024
    );
    assert!(
        converted["source"]["families"]["datum"]["batches"]
            .as_u64()
            .unwrap()
            > 0
    );
    drop(c);
    drop(original);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_source_statement_timeout_is_named_closed_and_retryable() -> Result {
    let f = fixture(223);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let barrier = source.barrier().await?;
    let child = target.start_cli(
        "convert",
        "mainnet",
        &[
            "--source",
            &source.dsn(true),
            "--horizon-block",
            "6",
            "--source-statement-timeout-seconds",
            "1",
        ],
    )?;
    source.wait_barrier().await?;
    let report = failure(&child.wait_with_output().await?)?;
    assert_eq!(report["error"], "source_statement_timeout");
    assert_eq!(report["family"], "block");
    assert_eq!(report["sqlstate"], "57014");
    assert!(report["elapsed_seconds"].as_f64().unwrap() >= 0.9);
    barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    target.closed_empty().await?;
    target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--source-statement-timeout-seconds",
                "60",
            ],
        )
        .await?;
    drop(barrier);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_destination_termination_stream_finish_and_post_source_commit_kill() -> Result {
    use tokio::io::AsyncReadExt;
    for mode in ["stream", "finish", "kill"] {
        let f = fixture(225);
        let source = Database::new().await?;
        source.legacy(&f).await?;
        let target = Database::new().await?;
        target.cli("create", "mainnet", &[]).await?;
        let owner = connect(&target.config).await?;
        if mode != "stream" {
            owner.batch_execute("CREATE FUNCTION dsa.test_barrier() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(726,2);RETURN NEW;END$$;CREATE TRIGGER test_barrier BEFORE INSERT ON dsa.first_served FOR EACH STATEMENT EXECUTE FUNCTION dsa.test_barrier()").await?;
            owner
                .query_one("SELECT pg_advisory_lock(726,2)", &[])
                .await?;
        }
        let mut child = target.start_cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--batch-rows",
                "2",
            ],
        )?;
        // Per-page progress must be consumed while the synchronization observer
        // runs, rather than letting a full pipe stop the child before its gate.
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let output = tokio::spawn(async move {
            let mut b = Vec::new();
            stdout.read_to_end(&mut b).await.map(|_| b)
        });
        let errors = tokio::spawn(async move {
            let mut b = Vec::new();
            stderr.read_to_end(&mut b).await.map(|_| b)
        });
        let started = Instant::now();
        let pid = loop {
            let sql = if mode == "stream" {
                "SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND query LIKE 'COPY pg_temp.block%' AND xact_start IS NOT NULL"
            } else {
                "SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND wait_event='advisory'"
            };
            if let Some(row) = target.admin.query_opt(sql, &[&target.name]).await? {
                break row.get::<_, i32>(0);
            }
            if started.elapsed() > Duration::from_secs(10) {
                return Err(format!("no destination {mode} barrier").into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let source_open:bool=source.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt-convert-source' AND xact_start IS NOT NULL)",&[&source.name]).await?.get(0);
        assert_eq!(
            source_open,
            mode == "stream",
            "source snapshot phase at {mode}"
        );
        if mode == "kill" {
            child.kill().await?;
        } else {
            target
                .admin
                .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                .await?;
        }
        if mode != "stream" {
            owner
                .query_one("SELECT pg_advisory_unlock(726,2)", &[])
                .await?;
        }
        let status = child.wait().await?;
        let stdout = output.await??;
        let stderr = errors.await??;
        assert!(
            !status.success(),
            "{} {}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        let released = Instant::now();
        loop {
            let held:bool=source.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt-convert-source' AND xact_start IS NOT NULL)",&[&source.name]).await?.get(0);
            if !held {
                break;
            }
            assert!(
                released.elapsed() < Duration::from_secs(3),
                "source snapshot leaked after {mode}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        target.closed_empty().await?;
        let temporary:i64=target.admin.query_one("SELECT count(*) FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.relpersistence='t' AND r.rolname=$1",&[&target.owner]).await?.get(0);
        assert_eq!(temporary, 0, "temporary staging leaked after {mode}");
        if mode != "stream" {
            owner.batch_execute("DROP TRIGGER test_barrier ON dsa.first_served;DROP FUNCTION dsa.test_barrier()").await?;
        }
        println!(
            "{}",
            json!({"event":"staged_conversion_cancellation","phase":mode,"source_transaction_released":true,"temporary_relations_remaining":temporary,"serving_partial":false})
        );
        let retried = target
            .cli(
                "convert",
                "mainnet",
                &["--source", &source.dsn(true), "--horizon-block", "6"],
            )
            .await?;
        let pristine = Database::new().await?;
        let oracle = pristine
            .cli(
                "convert",
                "mainnet",
                &["--source", &source.dsn(true), "--horizon-block", "6"],
            )
            .await?;
        assert_eq!(
            retried["verified"]["content_digest"],
            oracle["verified"]["content_digest"]
        );
        assert_eq!(
            retried["verified"]["state_digest"],
            oracle["verified"]["state_digest"]
        );
        drop(owner);
        pristine.cleanup().await?;
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

/// On-demand only: all data are deterministic/generated; normal branch CI
/// excludes this test. Hosted runs archive its full JSON plans and SQL hashes.
#[tokio::test]
#[ignore = "on-demand generated multi-million-header source cost"]
async fn convert_scaled_source_cost() -> Result {
    if std::env::var("DSA_CONVERSION_SUPPLY_ONLY").as_deref() == Ok("1") {
        return supply_source_realistic_cost().await;
    }
    let headers: u32 = std::env::var("DSA_CONVERSION_HEADERS")
        .unwrap_or_else(|_| "2432170".to_owned())
        .parse()?;
    assert!(headers >= 10000);
    let offset = 13_200_000;
    let mut f = fixture(227);
    f.translate_heights(offset);
    let p = offset + headers - 1;
    let t = p - f.profile.cardano.security_parameter;
    let h = t - 30009.min(headers / 10);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let began = Instant::now();
    // The real Mainnet source's physical header count is about1.2M, despite its
    // height near14M. Here header count, height, and density are all explicit.
    source.write(&format!("UPDATE serve.block SET epoch_no=(id-{offset})/20000,slot_no=id*20,time=timestamp '2026-01-01'+(id-{offset})*interval '20seconds'")).await?;
    let asset = f.asset_id("cnight");
    let mut counts = BTreeMap::new();
    for low in (offset..=p).step_by(5000) {
        let high = (low + 4999).min(p);
        source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5('block/'||n::text)||md5('block2/'||n::text),'hex'),(n-{offset})/20000,n*20,n,timestamp '2026-01-01'+(n-{offset})*interval '20seconds',0 FROM generate_series({low}::bigint,{high}::bigint)n ON CONFLICT DO NOTHING;
          INSERT INTO serve.tx SELECT n*4096+7,decode(md5('tx/'||n::text)||md5('tx2/'||n::text),'hex'),n,7,true FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.tx_out SELECT n*16777216+7*4096,n*4096+7,0,'external-'||(n%10007)::text,1,NULL,NULL FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.ma_tx_out SELECT (n*16777216+7*4096)*4096,1,n*16777216+7*4096,{asset} FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.ma_tx_out SELECT (n*16777216+7*4096)*4096+1,1,n*16777216+7*4096,9011 FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%10=0;
          INSERT INTO serve.tx SELECT (n+1)*4096+8,decode(md5('spend/'||n::text)||md5('spend2/'||n::text),'hex'),n+1,8,true FROM generate_series({low}::bigint,least({high},{h}-2)::bigint)n WHERE n%2=0 AND n%10<>0;
          INSERT INTO serve.tx_in SELECT (n+1)*16777216+8*4096,(n+1)*4096+8,n*4096+7,0 FROM generate_series({low}::bigint,least({high},{h}-2)::bigint)n WHERE n%2=0 AND n%10<>0")).await?;
        if (low - offset).is_multiple_of(100000) {
            println!(
                "{}",
                json!({"event":"cost_fixture_progress","last_header":high,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    let owner = connect(&source.config).await?;
    for table in TABLES {
        owner
            .batch_execute(&format!("ANALYZE serve.{table}"))
            .await?;
        counts.insert(
            table,
            owner
                .query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
                .await?
                .get::<_, i64>(0),
        );
    }
    assert_eq!(counts["block"], i64::from(headers));
    println!(
        "{}",
        json!({"event":"cost_fixture_ready","headers":headers,"physical_rows":counts,"H":h,"T":t,"P":p,"setup_seconds":began.elapsed().as_secs_f64()})
    );
    let mut config = source.reader_config();
    config.options("-c work_mem=4MB -c jit=off -c max_parallel_workers_per_gather=0");
    for limit in [None, Some(300)] {
        let target = Database::new().await?;
        let mut source_session = LegacySource::connect(config.clone(), None).await?;
        source_session.limits(limit, Some(300)).await?;
        source
            .admin
            .batch_execute("SELECT pg_stat_clear_snapshot()")
            .await?;
        let before: i64 = source
            .admin
            .query_one(
                "SELECT temp_bytes FROM pg_stat_database WHERE datname=$1",
                &[&source.name],
            )
            .await?
            .get(0);
        let started = Instant::now();
        let mut store = Store::connect(target.config.clone(), None).await?;
        let result = store
            .convert(
                &f.profile,
                &mut source_session,
                &ConvertOptions {
                    horizon_block: Some(h),
                    ..Default::default()
                },
            )
            .await;
        // Flush this generated source backend's statistics before measuring;
        // this does not write any served/source relation or alter server config.
        drop(source_session);
        tokio::time::sleep(Duration::from_millis(100)).await;
        source
            .admin
            .batch_execute("SELECT pg_stat_clear_snapshot()")
            .await?;
        let after: i64 = source
            .admin
            .query_one(
                "SELECT temp_bytes FROM pg_stat_database WHERE datname=$1",
                &[&source.name],
            )
            .await?
            .get(0);
        match result {
            Ok(result) => println!(
                "{}",
                json!({"event":"cost_conversion","explicit_statement_seconds":limit,"elapsed_seconds":started.elapsed().as_secs_f64(),"source_temp_bytes_delta":after-before,"result":result})
            ),
            Err(error) => {
                println!(
                    "{}",
                    json!({"event":"cost_conversion_failure","explicit_statement_seconds":limit,"elapsed_seconds":started.elapsed().as_secs_f64(),"source_temp_bytes_delta":after-before,"error":error.report()})
                );
                if limit.is_some() {
                    return Err(error.into());
                }
                assert_eq!(error.report()["error"], "source_statement_timeout");
                target.closed_empty().await?;
            }
        }
        drop(store);
        target.cleanup().await?;
    }
    let mut session = LegacySource::connect(config, None).await?;
    session.limits(Some(300), Some(300)).await?;
    let selection = dsa_store::keep::Selection::new(
        &f.profile,
        dsa_chain::keep::Bounds {
            horizon: h,
            target: t,
            tip: p,
        },
    )?;
    // Both K10 paths on the same generated source; no fixture normalization.
    for present in [false, true] {
        if present {
            source.write(&format!("INSERT INTO serve.tx VALUES({}::bigint*4096+8,decode('{}','hex'),{h},8,true);INSERT INTO serve.tx_in VALUES({}::bigint*16777216+8*4096,{}::bigint*4096+8,{}::bigint*4096+7,0)",h,"ac".repeat(32),h,h,offset)).await?;
        }
        let tx = session.snapshot().await?;
        let settings:Value=tx.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'work_mem',current_setting('work_mem'),'jit',current_setting('jit'),'parallel',current_setting('max_parallel_workers_per_gather'),'server_version',current_setting('server_version'))",&[]).await?.get(0);
        for table in [
            "block",
            "tx",
            "tx_out",
            "tx_in",
            "ma_tx_out",
            "datum",
            "multi_asset",
        ] {
            let sql = selection.conversion_rows_sql(table)?;
            let plan: Value = tx
                .query_one(
                    &format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}"),
                    &selection.parameters(),
                )
                .await?
                .get(0);
            println!(
                "{}",
                json!({"event":"cost_plan","family":table,"window_spend_present":present,"settings":settings,"sql_sha256":format!("{:x}",<sha2::Sha256 as sha2::Digest>::digest(sql.as_bytes())),"plan":plan})
            );
        }
        tx.commit().await?;
    }
    drop(session);
    drop(owner);
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_source_preflight_rejects_extra_rights_empty_spends_and_old_schema() -> Result {
    let f = fixture(229);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    source
        .admin
        .batch_execute(&format!(
            "GRANT TEMP ON DATABASE {} TO {}",
            source.name, source.reader
        ))
        .await?;
    target
        .refusal(
            "mainnet",
            &source.dsn(true),
            &[],
            "reader_privileges_not_exact",
        )
        .await?;
    source
        .admin
        .batch_execute(&format!(
            "REVOKE TEMP ON DATABASE {} FROM {}",
            source.name, source.reader
        ))
        .await?;
    source.write("DELETE FROM serve.tx_in").await?;
    target
        .refusal("mainnet", &source.dsn(true), &[], "source_has_no_spends")
        .await?;
    source.write(&format!("DROP VIEW public.epoch_param;CREATE VIEW public.epoch_param AS SELECT epoch_no,nonce FROM serve.epoch_param WHERE chain.gate();GRANT SELECT ON public.epoch_param TO {}",source.reader)).await?;
    target
        .refusal(
            "mainnet",
            &source.dsn(true),
            &[],
            "unsupported_legacy_schema",
        )
        .await?;
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_v2_historical_outpoint_tokens_and_original_ordinals_roundtrip() -> Result {
    // Generated ledger rows seeded with the two public Q18 unit/outpoint facts;
    // no historical dump, provider sample or fixed network digest is involved.
    let mut f = fixture(402);
    let facts = [
        (
            9020,
            "ics",
            "725c21c20c9193fe813d98cf886cd9e5e40187ac18cfc3be6e30bb349cbc90b4",
            "475db387ba92282a387e9ddfb7f8d2fd297f9b660450005f3ed37550",
        ),
        (
            9021,
            "reserve",
            "ed8a1f6afb4cb2b10e37e1092601a20a021da7231faafe10816f3c56bc3532d0",
            "fb964a2751bb9cb9c7a6d1475d1ea4a3960820a0f318883647502426",
        ),
    ];
    let mut originals = Vec::new();
    for (ordinal, (id, role, hash, policy)) in facts.into_iter().enumerate() {
        f.chain.assets.insert(
            id,
            dsa_chain::profiles::Asset {
                policy: policy.to_owned(),
                name: String::new(),
            },
        );
        let out = f.output(8, 8 + ordinal as u16 * 2, 2, role, "cnight", None);
        f.add_asset(out, id, 19);
        originals.push((out, hash, id));
    }
    f.reindex_ordinals();
    f.translate_heights(4_318_342);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let mut expected = selected(&f.rows(), &keep(&f.chain, &f.profile, f.bounds)?);
    for (out, hash, foreign) in originals {
        let (height, index, _) = dsa_chain::ids::output_position(out)?;
        let tx = dsa_chain::ids::transaction_id(height + 4_318_342, index)?;
        source
            .write(&format!(
                "UPDATE serve.tx SET hash=decode('{hash}','hex') WHERE id={tx}"
            ))
            .await?;
        for row in expected.get_mut("tx").unwrap() {
            if row["id"] == tx {
                row["hash"] = json!(format!("\\x{hash}"));
            }
        }
        assert!(
            !expected["ma_tx_out"]
                .iter()
                .any(|row| row["ident"] == foreign)
        );
        assert!(
            !expected["multi_asset"]
                .iter()
                .any(|row| row["id"] == foreign)
        );
        assert!(
            expected["tx_out"]
                .iter()
                .any(|row| row["tx_id"] == tx && row["index"] == 2)
        );
    }
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("v2.dsa");
    let target = Database::new().await?;
    let converted = target
        .cli(
            "convert",
            "preprod",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &f.bounds.horizon.to_string(),
                "--snapshot",
                file.to_str().unwrap(),
            ],
        )
        .await?;
    assert_eq!(target.rows().await?, expected);
    let status = target.cli("status", "preprod", &[]).await?;
    assert_eq!(status["identity"]["keep_rule_version"], 2);
    let restored = Database::new().await?;
    let result = restored
        .cli("restore", "preprod", &file_args(&file))
        .await?;
    assert_eq!(
        result["content_digest"],
        converted["verified"]["content_digest"]
    );
    assert_eq!(restored.rows().await?, expected);
    source.cleanup().await?;
    target.cleanup().await?;
    restored.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_supply_pages_sparse_dense_target_and_numeric_boundaries() -> Result {
    let mut f = fixture(401);
    let mut added = Vec::new();
    for n in 0..4300_u16 {
        let out = f.output(7, 100 + n / 4000, n % 4000, "other", "cnight", None);
        for asset in f
            .chain
            .output_assets
            .values_mut()
            .filter(|a| a.output == out)
        {
            asset.asset = 9001;
        }
        added.push(out);
    }
    let cn = f.asset_id("cnight");
    for out in &added[100..103] {
        for asset in f
            .chain
            .output_assets
            .values_mut()
            .filter(|a| a.output == *out)
        {
            asset.asset = cn;
        }
    }
    f.spend(7, 200, 0, added[100]);
    f.spend(8, 200, 0, added[101]);
    f.spend(9, 200, 0, added[102]);
    // An absent optional fixed descriptor must still produce a zero supply.
    let absent = f.asset_id("technical_committee");
    f.chain.output_assets.retain(|_, a| a.asset != absent);
    f.chain.assets.remove(&absent);
    f.reindex_ordinals();
    let boundary = f.chain.output_assets.keys().copied().nth(4095).unwrap();
    let after_boundary = f.chain.output_assets.keys().copied().nth(4096).unwrap();
    f.chain.output_assets.get_mut(&boundary).unwrap().asset = cn;
    f.chain
        .output_assets
        .get_mut(&after_boundary)
        .unwrap()
        .asset = cn;
    let source = Database::new().await?;
    source.legacy(&f).await?;
    source.write(&format!("UPDATE serve.ma_tx_out SET quantity=18446744073709551615 WHERE id IN ({boundary},{after_boundary})")).await?;
    let before = source.catalog().await?;
    let mut reader = LegacySource::connect(source.reader_config(), None).await?;
    let tx = reader.snapshot().await?;
    let units = dsa_chain::assets::fixed_units(&f.profile);
    let policies = units
        .iter()
        .map(|u| hex::decode(&u[..56]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let names = units
        .iter()
        .map(|u| hex::decode(&u[56..]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut expected = tx
        .query(
            include_str!("common/source_supply_unpaged.sql"),
            &[&policies, &names, &8_i64],
        )
        .await?
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        expected[&f.profile.assets["technical_committee"].unit()],
        "0"
    );
    assert!(expected[&f.profile.assets["cnight"].unit()].parse::<i128>()? > i128::from(u64::MAX));
    tx.batch_execute("SET LOCAL enable_seqscan=off;SET LOCAL enable_bitmapscan=off;SET LOCAL enable_indexscan=on;SET LOCAL enable_indexonlyscan=on").await?;
    let sql = include_str!("../../dsa-store/src/conversion_supply.sql");
    let mut previous = vec!["0".to_owned(); units.len()];
    let mut after = -1_i64;
    let mut total = 0_i64;
    let mut pages = 0;
    let mut actual: BTreeMap<String, String>;
    loop {
        let plan: Value = tx
            .query_one(
                &format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}"),
                &[&policies, &names, &8_i64, &after, &previous],
            )
            .await?
            .get(0);
        fn page_limit(v: &Value) -> bool {
            if let Some(o) = v.as_object() {
                if o.get("Subplan Name").and_then(Value::as_str) == Some("CTE page") {
                    fn ordered_index(v: &Value) -> bool {
                        if let Some(o) = v.as_object() {
                            if o.get("Relation Name").and_then(Value::as_str) == Some("ma_tx_out") {
                                return matches!(
                                    o.get("Node Type").and_then(Value::as_str),
                                    Some("Index Scan" | "Index Only Scan")
                                ) && o.get("Scan Direction").and_then(Value::as_str)
                                    == Some("Forward")
                                    && o["Actual Rows"].as_u64().unwrap() <= 4096;
                            }
                            return o.values().any(ordered_index);
                        }
                        v.as_array().is_some_and(|a| a.iter().any(ordered_index))
                    }
                    return o.get("Node Type").and_then(Value::as_str) == Some("Limit")
                        && o["Actual Rows"].as_u64().unwrap() <= 4096
                        && ordered_index(v);
                }
                return o.values().any(page_limit);
            }
            v.as_array().is_some_and(|a| a.iter().any(page_limit))
        }
        assert!(page_limit(&plan), "unbounded raw asset page: {plan}");
        let rows = tx
            .query(sql, &[&policies, &names, &8_i64, &after, &previous])
            .await?;
        let count: i64 = rows[0].get(3);
        let last: Option<i64> = rows[0].get(2);
        assert!(count <= 4096);
        actual = rows
            .into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();
        previous = units.iter().map(|u| actual[u].clone()).collect();
        total += count;
        pages += 1;
        println!(
            "{}",
            json!({"event":"supply_page_plan","page":pages,"rows":count,"after_id":after,"last_id":last,"plan":plan})
        );
        if let Some(id) = last {
            assert!(id > after);
            after = id;
        } else {
            break;
        }
    }
    assert_eq!(actual, expected);
    assert!(pages >= 3);
    let total_expected:i64=tx.query_one("SELECT count(*) FROM public.ma_tx_out WHERE id BETWEEN 0 AND 8::bigint*68719476736+68719476735",&[]).await?.get(0);
    assert_eq!(total, total_expected);
    // PostgreSQL numeric accumulation also accepts a prior sum wider than u128.
    let wide = "340282366920938463463374607431768211456".to_owned();
    let previous = vec![wide.clone(); units.len()];
    let result = tx
        .query(sql, &[&policies, &names, &8_i64, &after, &previous])
        .await?;
    assert!(result.iter().all(|r| r.get::<_, String>(1) == wide));
    tx.commit().await?;
    drop(reader);
    // Direct page arithmetic accepts wider numeric accumulators; a served
    // store's existing supply domain is uint64, so keep this CLI case valid
    // while still exceeding signed bigint.
    source.write(&format!("UPDATE serve.ma_tx_out SET quantity=5000000000000000000 WHERE id IN ({boundary},{after_boundary})")).await?;
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let snapshot = session.snapshot().await?;
    expected = snapshot
        .query(
            include_str!("common/source_supply_unpaged.sql"),
            &[&policies, &names, &8_i64],
        )
        .await?
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();
    assert!(expected[&f.profile.assets["cnight"].unit()].parse::<i128>()? > i128::from(i64::MAX));
    snapshot.commit().await?;
    drop(session);
    let target = Database::new().await?;
    let source_dsn = format!(
        "{} options='-c enable_seqscan=on -c enable_bitmapscan=on -c enable_indexscan=off -c enable_indexonlyscan=off'",
        source.dsn(true)
    );
    let output = target
        .start_cli(
            "convert",
            "mainnet",
            &["--source", &source_dsn, "--horizon-block", "6"],
        )?
        .wait_with_output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let converted: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(converted["verified"]["supply"], json!(expected));
    assert_eq!(converted["source"]["supply_rows"], total_expected);
    assert_eq!(converted["source"]["supply_batches"], pages);
    assert_eq!(converted["source"]["settings"]["statement_timeout"], "1min");
    assert!(
        converted["source"]["longest_supply_page_seconds"]
            .as_f64()
            .unwrap()
            < 60.0
    );
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        if let Ok(event) = serde_json::from_str::<Value>(line) {
            match event["event"].as_str() {
                Some("source_supply_page_finished") => {
                    assert!(event["rows"].as_i64().unwrap() <= 4096)
                }
                Some("source_supply_started") => {
                    assert_eq!(event["planner_before"], json!(["on", "on", "off", "off"]))
                }
                Some("source_supply_finished") => {
                    assert_eq!(event["planner_restored"], json!(["on", "on", "off", "off"]))
                }
                _ => {}
            }
        }
    }
    assert_eq!(source.catalog().await?, before);
    println!(
        "{}",
        json!({"event":"supply_page_cli_regression","rows":total_expected,"pages":pages,"boundary_id":boundary,"after_boundary_id":after_boundary,"source":converted["source"],"supply":expected})
    );
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

// On-demand hosted cost branch of the existing skipped scale harness. Generated
// data only, genuine legacy DDL/gate/grants, no operator database or provider.
async fn supply_source_realistic_cost() -> Result {
    let wanted: i64 = std::env::var("DSA_SUPPLY_ROWS")
        .unwrap_or_else(|_| "2630984".to_owned())
        .parse()?;
    assert!(wanted >= 10000);
    let offset = 9_000_000_u32;
    let headers = 1_202_855_u32;
    let mut f = fixture(401);
    f.translate_heights(offset);
    let p = offset + headers - 1;
    let t = p - f.profile.cardano.security_parameter;
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let units = dsa_chain::assets::fixed_units(&f.profile);
    let policies = units
        .iter()
        .map(|u| hex::decode(&u[..56]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let names = units
        .iter()
        .map(|u| hex::decode(&u[56..]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let old = include_str!("common/source_supply_unpaged.sql");
    let sql = include_str!("../../dsa-store/src/conversion_supply.sql");
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = session.snapshot().await?;
    let mut expected = tx
        .query(old, &[&policies, &names, &i64::from(t)])
        .await?
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect::<BTreeMap<_, _>>();
    tx.commit().await?;
    drop(session);
    let owner = connect(&source.config).await?;
    let original_assets: i64 = owner
        .query_one("SELECT count(*) FROM serve.ma_tx_out", &[])
        .await?
        .get(0);
    let original_inputs: i64 = owner
        .query_one("SELECT count(*) FROM serve.tx_in", &[])
        .await?
        .get(0);
    let generated = wanted - original_assets;
    let spent = (2_386_344 - original_inputs).min(generated * 95 / 100);
    let asset = f.asset_id("cnight");
    let began = Instant::now();
    for low in (i64::from(offset)..=i64::from(p)).step_by(10000) {
        let high = (low + 9999).min(i64::from(p));
        source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5('supplyblock/'||n::text)||md5('supplyblock2/'||n::text),'hex'),(n-{offset})/20000,n*20,n,timestamp '2026-01-01'+(n-{offset})*interval '20 seconds',0 FROM generate_series({low}::bigint,{high}::bigint)n ON CONFLICT DO NOTHING")).await?;
    }
    for low in (0..generated).step_by(10000) {
        let high = (low + 9999).min(generated - 1);
        source.write(&format!("WITH coordinates AS(SELECT DISTINCT {offset}+13+n/3 h FROM generate_series({low}::bigint,{high}::bigint)n) INSERT INTO serve.tx SELECT h*4096+100,decode(md5('supplytx/'||h::text)||md5('supplytx2/'||h::text),'hex'),h,100,true FROM coordinates ON CONFLICT DO NOTHING;
        INSERT INTO serve.tx_out SELECT ({offset}+13+n/3)*16777216+100*4096+n%3,({offset}+13+n/3)*4096+100,(n%3)::smallint,'holder-'||(n%10007)::text,1,NULL,NULL FROM generate_series({low}::bigint,{high}::bigint)n;
        INSERT INTO serve.ma_tx_out SELECT (({offset}+13+n/3)*16777216+100*4096+n%3)*4096,1,({offset}+13+n/3)*16777216+100*4096+n%3,{asset} FROM generate_series({low}::bigint,{high}::bigint)n;
        WITH coordinates AS(SELECT DISTINCT {offset}+14+n/3 h FROM generate_series({low}::bigint,least({high}::bigint,{spent}::bigint-1))n) INSERT INTO serve.tx SELECT h*4096+101,decode(md5('supplyspend/'||h::text)||md5('supplyspend2/'||h::text),'hex'),h,101,true FROM coordinates ON CONFLICT DO NOTHING;
        INSERT INTO serve.tx_in SELECT ({offset}+14+n/3)*16777216+101*4096+n%3,({offset}+14+n/3)*4096+101,({offset}+13+n/3)*4096+100,(n%3)::smallint FROM generate_series({low}::bigint,least({high}::bigint,{spent}::bigint-1))n")).await?;
        if low % 100000 == 0 {
            println!(
                "{}",
                json!({"event":"supply_cost_setup","rows":high+1,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    let mut counts = BTreeMap::new();
    let mut sizes = BTreeMap::new();
    for table in TABLES {
        owner
            .batch_execute(&format!("ANALYZE serve.{table}"))
            .await?;
        counts.insert(
            table,
            owner
                .query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
                .await?
                .get::<_, i64>(0),
        );
        sizes.insert(
            table,
            owner
                .query_one(
                    "SELECT pg_total_relation_size($1::text::regclass)",
                    &[&format!("serve.{table}")],
                )
                .await?
                .get::<_, i64>(0),
        );
    }
    assert_eq!(counts["ma_tx_out"], wanted);
    assert_eq!(counts["block"], i64::from(headers));
    let u = f.profile.assets["cnight"].unit();
    expected.insert(
        u.clone(),
        (expected[&u].parse::<i128>()? + i128::from(generated - spent)).to_string(),
    );
    println!(
        "{}",
        json!({"event":"supply_cost_fixture","counts":counts,"bytes":sizes,"T":t,"P":p,"generated_rows":generated,"generated_spends":spent,"setup_seconds":began.elapsed().as_secs_f64(),"expected_supply":expected})
    );
    // Preserve the previous whole-history plan and actual outcome at the SAME
    // 60s role limit. A hosted machine may finish it; no timeout is required.
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = session.snapshot().await?;
    let settings:Value=tx.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'jit',current_setting('jit'),'read_only',current_setting('transaction_read_only'),'isolation',current_setting('transaction_isolation'),'server_version',current_setting('server_version'))",&[]).await?.get(0);
    assert_eq!(settings["statement_timeout"], "1min");
    let plan: Value = tx
        .query_one(
            &format!("EXPLAIN(SETTINGS,FORMAT JSON) {old}"),
            &[&policies, &names, &i64::from(t)],
        )
        .await?
        .get(0);
    println!(
        "{}",
        json!({"event":"supply_cost_old_plan","settings":settings,"plan":plan})
    );
    let began = Instant::now();
    match tx.query(old, &[&policies, &names, &i64::from(t)]).await {
        Ok(rows) => {
            let actual = rows
                .into_iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual, expected);
            println!(
                "{}",
                json!({"event":"supply_cost_old","outcome":"complete","elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
        Err(error) => {
            assert_eq!(error.code().map(|c| c.code()), Some("57014"));
            println!(
                "{}",
                json!({"event":"supply_cost_old","outcome":"57014","elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    tx.rollback().await?;
    drop(session);
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = session.snapshot().await?;
    tx.batch_execute("SET LOCAL enable_seqscan=off;SET LOCAL enable_bitmapscan=off;SET LOCAL enable_indexscan=on;SET LOCAL enable_indexonlyscan=on").await?;
    let mut previous = vec!["0".to_owned(); units.len()];
    let mut after = -1_i64;
    let mut seen = 0_i64;
    let mut pages = 0;
    let mut longest = 0.0_f64;
    let began = Instant::now();
    let mut actual: BTreeMap<String, String>;
    loop {
        if pages == 0 || seen >= wanted / 2 && seen - 4096 < wanted / 2 || seen + 4096 >= wanted {
            let plan: Value = tx
                .query_one(
                    &format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}"),
                    &[&policies, &names, &i64::from(t), &after, &previous],
                )
                .await?
                .get(0);
            println!(
                "{}",
                json!({"event":"supply_cost_page_plan","page":pages,"after_id":after,"seen":seen,"plan":plan})
            );
        }
        let started = Instant::now();
        let rows = tx
            .query(sql, &[&policies, &names, &i64::from(t), &after, &previous])
            .await?;
        let elapsed = started.elapsed().as_secs_f64();
        longest = longest.max(elapsed);
        let count: i64 = rows[0].get(3);
        let last: Option<i64> = rows[0].get(2);
        assert!((0..=4096).contains(&count));
        assert!(elapsed < 60.0);
        actual = rows
            .into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();
        previous = units.iter().map(|u| actual[u].clone()).collect();
        pages += 1;
        seen += count;
        println!(
            "{}",
            json!({"event":"supply_cost_page","page":pages,"rows":count,"after_id":after,"last_id":last,"elapsed_seconds":elapsed})
        );
        if let Some(id) = last {
            assert!(id > after);
            after = id;
        } else {
            break;
        }
    }
    assert_eq!(actual, expected);
    assert_eq!(seen, wanted);
    assert!(pages > 1);
    let final_settings: String = tx.query_one("SHOW statement_timeout", &[]).await?.get(0);
    assert_eq!(final_settings, "1min");
    println!(
        "{}",
        json!({"event":"supply_cost_result","rows":seen,"pages":pages,"elapsed_seconds":began.elapsed().as_secs_f64(),"longest_page_seconds":longest,"settings":settings,"final_statement_timeout":final_settings,"supply":actual,"status":"PASS"})
    );
    tx.commit().await?;
    drop(session);
    drop(owner);
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_binary_staging_preserves_nulls_numeric_and_oversized_row() -> Result {
    let mut f = fixture(403);
    for (tx, datum) in [(25, 90301), (26, 90302), (27, 90303)] {
        f.output(7, tx, 0, "mapping", "auth", Some(datum));
    }
    for tx in [20, 21] {
        let output = f.output(7, tx, 0, "ics", "bridge", None);
        let transaction = f.chain.outputs[&output].transaction;
        f.chain.metadata.insert(transaction, transaction);
    }
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let hash = |id| f.chain.datums[&id].hash.clone();
    let tx0 = 7_i64 * 4096 + 20;
    let tx1 = 7_i64 * 4096 + 21;
    source.write(&format!("UPDATE serve.datum SET value=NULL WHERE hash=decode('{}','hex');UPDATE serve.datum SET value='null'::jsonb WHERE hash=decode('{}','hex');UPDATE serve.datum SET value=jsonb_build_object('bytes',repeat('x',10000),'wide',340282366920938463463374607431768211456::numeric) WHERE hash=decode('{}','hex');UPDATE serve.tx_metadata SET json=NULL WHERE id={tx0};UPDATE serve.tx_metadata SET json='null'::jsonb WHERE id={tx1}",hash(90301),hash(90302),hash(90303))).await?;
    let target = Database::new().await?;
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("typed.dsa");
    let converted = target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--batch-bytes",
                "1024",
                "--batch-rows",
                "64",
                "--snapshot",
                file.to_str().unwrap(),
            ],
        )
        .await?;
    assert!(
        converted["source"]["raw_families"]["datum"]["largest_row_bytes"]
            .as_u64()
            .unwrap()
            > 10000
    );
    let restored = Database::new().await?;
    restored
        .cli("restore", "mainnet", &file_args(&file))
        .await?;
    for table in ["datum", "tx_metadata"] {
        let (field, filter) = if table == "datum" {
            (
                "value",
                format!(
                    "hash IN(decode('{}','hex'),decode('{}','hex'),decode('{}','hex'))",
                    hash(90301),
                    hash(90302),
                    hash(90303)
                ),
            )
        } else {
            ("json", format!("id IN({tx0},{tx1})"))
        };
        let query = format!(
            "SELECT jsonb_build_array(id,{field} IS NULL,{field}::text) FROM serve.{table} WHERE {filter} ORDER BY id"
        );
        let original = connect(&source.config)
            .await?
            .query(&query, &[])
            .await?
            .into_iter()
            .map(|r| r.get::<_, Value>(0))
            .collect::<Vec<_>>();
        assert!(!original.is_empty());
        assert!(original.iter().any(|v| v[1] == true));
        assert!(original.iter().any(|v| v[1] == false && v[2] == "null"));
        for database in [&target, &restored] {
            let rows = connect(&database.config)
                .await?
                .query(&query, &[])
                .await?
                .into_iter()
                .map(|r| r.get::<_, Value>(0))
                .collect::<Vec<_>>();
            assert_eq!(rows, original);
        }
    }
    println!(
        "{}",
        json!({"event":"binary_staging_typed_roundtrip","oversized_datum_wire_bytes":converted["source"]["raw_families"]["datum"]["largest_row_bytes"],"sql_null_and_json_null":"distinct","snapshot_restore":"PASS","source":converted["source"]})
    );
    source.cleanup().await?;
    target.cleanup().await?;
    restored.cleanup().await?;
    Ok(())
}
