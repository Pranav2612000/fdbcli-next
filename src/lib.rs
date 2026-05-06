use foundationdb::directory::{Directory, DirectoryError, DirectoryLayer, DirectoryOutput};
use foundationdb::tuple::{pack, unpack};
use foundationdb::{Database, FdbResult, RangeOption, Transaction};
use std::env;

pub const ENV_DB_PATH: &str = "FDBCLI_DB_PATH";

/// Connect to FDB using env var or default cluster file.
///
/// - If `FDBCLI_DB_PATH` is set, it's treated as a *cluster file path*.
///   Example:
///     export FDBCLI_DB_PATH=/usr/local/etc/foundationdb/fdb.cluster
///
/// - Otherwise `Database::default()` is used.
pub fn connect_db() -> FdbResult<Database> {
    if let Ok(path) = env::var(ENV_DB_PATH) {
        Database::from_path(&path)
    } else {
        Database::default()
    }
}

/// Dump an entire directory (bounded by `limit`).
pub async fn dump_dir(trx: &Transaction, dir: &DirectoryOutput, limit: i32) -> FdbResult<()> {
    let (begin, end) = dir.range().expect("dir.range()");
    let range = RangeOption::from((begin.as_slice(), end.as_slice()));
    let kvs = trx
        .get_range(&range, limit.try_into().unwrap(), false)
        .await?;

    if kvs.is_empty() {
        println!("  (empty)");
    } else {
        for kv in kvs.iter() {
            println!(
                "  key = {:?}, value = {}",
                kv.key(),
                String::from_utf8_lossy(kv.value())
            );
        }
    }

    Ok(())
}

// =====================================================================
// Generic directory + tuple helpers (for dynamic REPL commands)
// =====================================================================

/// Generic: create or open a directory like ["app"], ["app","users"]
pub async fn dir_create(
    trx: &Transaction,
    path: &[&str],
) -> Result<DirectoryOutput, DirectoryError> {
    let layer = DirectoryLayer::default();
    let vec_path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
    let out = layer.create_or_open(trx, &vec_path, None, None).await?;
    Ok(out)
}

/// Generic: open existing directory; `path` may be empty for root dir layer.
pub async fn dir_open(trx: &Transaction, path: &[&str]) -> Result<DirectoryOutput, DirectoryError> {
    let layer = DirectoryLayer::default();
    let vec_path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
    let out = layer.open(trx, &vec_path, None).await?;
    Ok(out)
}

/// List child directories under `path` (or root if `path` is empty).
pub async fn dir_list(trx: &Transaction, path: &[&str]) -> Result<Vec<String>, DirectoryError> {
    let layer = DirectoryLayer::default();
    let vec_path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
    let children = layer.list(trx, &vec_path).await?;
    Ok(children)
}

/// Remove a directory at the given path.
/// This will recursively remove all subdirectories and keys within the directory.
pub async fn dir_remove(trx: &Transaction, path: &[&str]) -> Result<bool, DirectoryError> {
    let layer = DirectoryLayer::default();
    let vec_path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
    let removed = layer.remove(trx, &vec_path).await?;
    Ok(removed)
}

// ---- simple tuple support for REPL prefix / pack / unpack ----

/// Very simple tuple parser: expects "(value)" and uses it as a single-string tuple.
/// You can extend this later to handle multiple elements, ints, etc.
fn parse_simple_tuple_str(tuple_str: &str) -> Result<String, String> {
    let s = tuple_str.trim();
    if !s.starts_with('(') || !s.ends_with(')') {
        return Err(format!("Expected (value), got: {}", s));
    }
    let inner = &s[1..s.len() - 1]; // strip '(' and ')'
    Ok(inner.trim().to_string())
}

/// Generic prefix range: given a directory and a tuple string like "(user-1)",
/// build [begin, end) so you can scan by prefix.
pub fn tuple_prefix_range(
    dir: &DirectoryOutput,
    tuple_str: &str,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let v = parse_simple_tuple_str(tuple_str)?;

    let prefix = dir
        .pack(&(v.as_str(),))
        .map_err(|e| format!("pack error: {:?}", e))?;
    let mut end = prefix.clone();
    end.push(0xFF);

    Ok((prefix, end))
}

/// Build a *single key* from a tuple string, e.g. "(user-1)".
pub fn tuple_key_from_string(dir: &DirectoryOutput, tuple_str: &str) -> Result<Vec<u8>, String> {
    let v = parse_simple_tuple_str(tuple_str)?;
    let key = dir
        .pack(&(v.as_str(),))
        .map_err(|e| format!("pack error: {:?}", e))?;
    Ok(key)
}

/// Pack a tuple string into bytes *without* a directory (global tuple).
/// Currently supports "(value)" as single string.
pub fn tuple_pack_from_string(tuple_str: &str) -> Result<Vec<u8>, String> {
    let v = parse_simple_tuple_str(tuple_str)?;
    let bytes = pack(&(v.as_str(),));
    Ok(bytes)
}

/// Unpack raw bytes into a simple tuple string like "(value)".
pub fn tuple_unpack_to_string(bytes: &[u8]) -> Result<String, String> {
    let t: (String,) = unpack(bytes).map_err(|e| format!("unpack error: {:?}", e))?;
    Ok(format!("({})", t.0))
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use foundationdb::api::FdbApiBuilder;

    // -----------------------
    // Pure tuple tests (no FDB server required)
    // -----------------------

    #[test]
    fn tuple_pack_and_unpack_roundtrip() {
        let input = "(alice)";
        let bytes = tuple_pack_from_string(input).expect("pack should succeed");
        let out = tuple_unpack_to_string(&bytes).expect("unpack should succeed");
        assert_eq!(out, "(alice)");
    }

    #[test]
    fn tuple_pack_rejects_invalid_format() {
        // Missing parentheses
        assert!(tuple_pack_from_string("alice").is_err());
        // Bad parentheses
        assert!(tuple_pack_from_string("(alice").is_err());
        assert!(tuple_pack_from_string("alice)").is_err());
    }

    // -----------------------
    // Single FDB-backed test (requires FDB server)
    //
    // Marked #[ignore] so it does NOT run by default.
    // Run it manually with:
    //   cargo test -- --ignored
    // -----------------------

    #[tokio::test]
    #[ignore]
    async fn fdb_end_to_end_directory_and_prefix() {
        // Boot FDB network once for this process
        let _network = unsafe {
            FdbApiBuilder::default()
                .build()
                .expect("build FDB API")
                .boot()
                .expect("boot network")
        };

        let db = connect_db().expect("connect_db()");

        // ---------- 1) directory create + list ----------
        {
            let trx = db.create_trx().expect("create_trx");
            let path = ["test-root", "sub"];
            let _dir = dir_create(&trx, &path).await.expect("dir_create");
            trx.commit().await.expect("commit create");
        }

        {
            let trx2 = db.create_trx().expect("create_trx 2");
            let children = dir_list(&trx2, &["test-root"]).await.expect("dir_list");
            assert!(
                children.contains(&"sub".to_string()),
                "expected 'sub' in children: {:?}",
                children
            );
            // no mutation, but ok to commit
            trx2.commit().await.expect("commit list");
        }

        // ---------- 2) tuple_prefix_range sanity on real DirectoryOutput ----------
        {
            let trx3 = db.create_trx().expect("create_trx 3");
            // open the same directory we created above
            let dir = dir_open(&trx3, &["test-root", "sub"])
                .await
                .expect("dir_open for prefix test");

            let (begin, end) = tuple_prefix_range(&dir, "(alice)").expect("tuple_prefix_range");
            assert!(!begin.is_empty(), "begin should not be empty");
            assert!(begin.len() < end.len(), "end should be longer than begin");
            assert_eq!(end.last().copied(), Some(0xFF), "end should end with 0xFF");

            trx3.commit().await.expect("commit prefix test");
        }
    }
}
