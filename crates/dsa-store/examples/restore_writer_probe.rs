//! Generated-only API regression entry for the independent PG17 driver.
//! Takes the same DSA_TEST_DATABASE convention as integration tests. It never
//! prints a DSN; tuple changes roll back, sequence allocation is nontransactional.
use dsa_chain::profiles::Profile;
use dsa_store::{Store, storage};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
    let network = std::env::var("DSA_TEST_NETWORK").unwrap_or_else(|_| "preprod".into());
    let profile = Profile::load(&network)?;
    let mut store = Store::connect(config, None).await?;
    store.open(&profile).await?;
    if std::env::args().any(|arg| arg == "--follow-closed") {
        let before = store.status(&profile).await?;
        assert_eq!(before["position"]["serving"], false);
        assert_eq!(before["restore"]["phase"], "published");
        assert!(before["restore"]["retention_anchor_at"].is_null());
        let error = match store.following_transaction().await {
            Ok(tx) => {
                tx.rollback().await?;
                panic!("closed gate unexpectedly admitted a following transaction");
            }
            Err(error) => error.report(),
        };
        assert_eq!(error["sqlstate"], "55000");
        let after = store.status(&profile).await?;
        assert_eq!(before["position"], after["position"]);
        assert_eq!(after["restore"]["phase"], "complete");
        assert!(after["restore"]["retention_anchor_at"].is_string());
        assert!(!store.settle_restored_retention(&profile).await?);
        println!(
            "{}",
            json!({"passed":true,"actual_store_api":true,"following_refused_sqlstate":"55000","anchor_committed_before_gate_error":true,"gate_and_position_preserved":true,"restore":after.get("restore")})
        );
        return Ok(());
    }
    if std::env::args().any(|arg| arg == "--settle-only") {
        let before = store.status(&profile).await?;
        let settled = store.settle_restored_retention(&profile).await?;
        let after = store.status(&profile).await?;
        assert_eq!(before["position"], after["position"]);
        println!(
            "{}",
            json!({"passed":true,"actual_store_api":true,"retention_settled":settled,"gate_and_position_preserved":true,"restore":after.get("restore")})
        );
        return Ok(());
    }
    let before = store.verify(&profile).await?;
    let tx = store.following_transaction().await?;
    let table = storage::write_relation(&tx, "multi_asset").await?;
    let existing: i64 = tx
        .query_one(&format!("SELECT min(id) FROM {table}"), &[])
        .await?
        .get(0);
    let duplicate = tx
        .execute(
            &format!(
                "INSERT INTO {table} SELECT * FROM {table} WHERE id=$1 ON CONFLICT(id) DO NOTHING"
            ),
            &[&existing],
        )
        .await?;
    assert_eq!(duplicate, 0);
    let id: i64 = tx
        .query_one("SELECT nextval('dsa.multi_asset_id_seq')", &[])
        .await?
        .get(0);
    let maximum: i64 = tx
        .query_one(&format!("SELECT max(id) FROM {table}"), &[])
        .await?
        .get(0);
    assert!(id > maximum);
    let inserted:i64=tx.query_one(&format!("INSERT INTO {table}(id,policy,name) VALUES($1,decode(repeat('73',28),'hex'),decode('727573742d7772697465722d70726f6265','hex')) RETURNING id"),&[&id]).await?.get(0);
    assert_eq!(inserted, id);
    let visible: i64 = tx
        .query_one("SELECT count(*) FROM serve.multi_asset WHERE id=$1", &[&id])
        .await?
        .get(0);
    assert_eq!(visible, 1);
    tx.batch_execute("SAVEPOINT parent_guard").await?;
    let parent_error = tx
        .execute(
            "INSERT INTO serve.multi_asset SELECT * FROM serve.multi_asset WHERE id=$1",
            &[&existing],
        )
        .await
        .unwrap_err();
    let parent_sqlstate = parent_error.code().map(|c| c.code().to_owned());
    tx.batch_execute("ROLLBACK TO SAVEPOINT parent_guard")
        .await?;
    if table.starts_with("dsa_restore.") {
        // Current constructors use0A000; known earlier owned prototype layouts
        // preserve their original55000 guard without rewriting stored objects.
        assert!(matches!(
            parent_sqlstate.as_deref(),
            Some("0A000" | "55000")
        ));
    } else {
        assert_eq!(parent_sqlstate.as_deref(), Some("23505"));
    }
    let clock = storage::first_served_at(&tx, i64::from(before.state.horizon.height)).await?;
    assert!(clock.is_some());
    let target_clock = storage::first_served_at(&tx, i64::from(before.state.target.height)).await?;
    let restored_cohort: bool = tx
        .query_one("SELECT to_regclass('dsa.restore_build') IS NOT NULL", &[])
        .await?
        .get(0);
    if restored_cohort {
        assert_eq!(target_clock, clock);
    }
    let next_height = i64::from(before.state.target.height) + 1;
    // Explicit uncommitted clock model, not a real follower/target advance.
    tx.execute("INSERT INTO dsa.first_served(height,first_served_at) VALUES($1::bigint::integer,clock_timestamp()) ON CONFLICT(height) DO NOTHING",&[&next_height]).await?;
    let ordinary_clock:Option<String>=tx.query_one("SELECT to_char(first_served_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM dsa.first_served WHERE height=$1::bigint",&[&next_height]).await?.get(0);
    let next_clock = storage::first_served_at(&tx, next_height).await?;
    assert_eq!(next_clock, ordinary_clock);
    tx.rollback().await?;
    let after = store.verify(&profile).await?;
    assert_eq!(before.content_digest, after.content_digest);
    assert_eq!(before.state_digest, after.state_digest);
    assert_eq!(before.supply, after.supply);
    println!(
        "{}",
        json!({"passed":true,"write_relation":table,"actual_store_api":true,"existing_id_on_conflict_ignored":true,"allocated_id_above_imported":true,"returning_id_exact":true,"node_visible_once":true,"parent_sqlstate":parent_sqlstate,"retention_clock_present":true,"restored_cohort_present":restored_cohort,"cohort_horizon_clock":clock,"cohort_target_clock":target_clock,"next_height_clock":next_clock,"ordinary_next_clock_model_exact":true,"real_follower_advance":false,"tuples_rolled_back":true,"sequence_allocation_nontransactional":true,"content_state_supply_unchanged":true})
    );
    Ok(())
}
