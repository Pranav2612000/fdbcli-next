use clap::{Parser, Subcommand};
use foundationdb::api::FdbApiBuilder;
use foundationdb::options::StreamingMode;
use foundationdb::{Database, FdbResult, RangeOption};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context, Editor, Helper};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::runtime::Handle;

use fdbcli_next::{
    connect_db, dir_create, dir_list, dir_open, dir_remove, tuple_key_from_string,
    tuple_pack_from_string, tuple_prefix_range, tuple_unpack_to_string, utils::readable_key,
};

use hex;

/// Generic FoundationDB Directory/Tuple CLI
///
/// Uses env FDBCLI_DB_PATH as cluster file path if set.
/// Example:
///   export FDBCLI_DB_PATH=/usr/local/etc/foundationdb/fdb.cluster
#[derive(Parser, Debug)]
#[command(name = "fdbcli-next")]
#[command(about = "FoundationDB Directory/Tuple CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start interactive shell (REPL)
    Repl,
}

#[tokio::main]
async fn main() -> FdbResult<()> {
    let cli = Cli::parse();

    // Start FDB network
    let _network = unsafe { FdbApiBuilder::default().build()?.boot()? };
    let db = connect_db()?;

    match cli.command {
        Commands::Repl => cmd_repl(&db).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------

/// Get the history file path, preferring XDG_DATA_HOME if set
fn history_file_path() -> PathBuf {
    if let Ok(xdg_data) = std::env::var("XDG_DATA_HOME") {
        let mut path = PathBuf::from(xdg_data);
        path.push("fdbcli-next");
        // Create directory if it doesn't exist
        let _ = std::fs::create_dir_all(&path);
        path.push("history.txt");
        path
    } else if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".fdbcli-next_history");
        path
    } else {
        // Fallback to current directory
        PathBuf::from(".fdbcli-next_history")
    }
}

/// Print help information
fn print_help() {
    println!(
        "Commands:\n\
         # Directory layer\n\
         - cd <path...>               # change current directory\n\
         - cd ..                      # go up one directory\n\
         - cd /                       # go to root directory\n\
         - pwd                        # print current directory\n\
         - dircreate <path...>        # create directory\n\
         - dirlist [path...]          # list directories\n\
         - ls [path...]               # alias for dirlist\n\
         - rmdir <path...>            # remove directory and its contents\n\
                                      # supports wildcard: rmdir test*\n\
         \n\
         # Tuple / key helpers\n\
         - keypack (value)            # prints hex\n\
         - keyunpack <hex>            # prints (value)\n\
         \n\
         # Range & delete by tuple prefix\n\
         - clearprefix <path...> (tuple)\n\
         - range <path...> (tuple)\n\
         - getallvalues [path...]     # get all key-value pairs in directory\n\
         - subspacelist [path...]     # list subspaces in directory\n\
         - subspaceset <subspacename> # Set a current subspace to use\n\
         - setkey (tuple) value                      # set key in current directory\n\
         - setkey --subspace name (tuple) value      # set key in existing subspace\n\
         - delkey <path...> (tuple)   # delete single key\n\
         \n\
         - help\n\
         - quit / exit\n\
         \n\
         Navigation:\n\
         - UP/DOWN arrows: Navigate command history\n\
         - LEFT/RIGHT arrows: Move cursor\n\
         - Ctrl-C: Cancel current line\n\
         - Ctrl-D: Exit REPL\n"
    );
}

/// Resolve a path relative to the current directory
/// If args is empty, returns current_dir
/// If args contains "..", goes up one level
/// Otherwise, appends args to current_dir
fn resolve_path(current_dir: &[String], args: &[&str]) -> Vec<String> {
    if args.is_empty() {
        return current_dir.to_vec();
    }

    let mut result = current_dir.to_vec();

    for arg in args {
        // Strip trailing slashes from the argument
        for path in arg.split("/") {
            let path_trimmed = path.trim_end_matches('/');

            if path_trimmed == ".." {
                result.pop();
            } else if !path_trimmed.is_empty() {
                result.push(path_trimmed.to_string());
            }
        }
    }

    result
}

/// Match a string against a pattern with wildcards (*)
/// Examples:
///   match_pattern("test123", "test*") => true
///   match_pattern("test123", "*123") => true
///   match_pattern("test123", "test*123") => true
///   match_pattern("hello", "test*") => false
fn match_pattern(text: &str, pattern: &str) -> bool {
    // If no wildcard, just do exact match
    if !pattern.contains('*') {
        return text == pattern;
    }

    // Split pattern by '*' to get parts that must match
    let parts: Vec<&str> = pattern.split('*').collect();

    // Empty pattern or just "*" matches everything
    if parts.len() == 1 && parts[0].is_empty() {
        return true;
    }

    let mut text_pos = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        // First part must match at the start
        if i == 0 {
            if !text.starts_with(part) {
                return false;
            }
            text_pos = part.len();
            continue;
        }

        // Last part must match at the end
        if i == parts.len() - 1 {
            return text.ends_with(part) && text.len() >= text_pos + part.len();
        }

        // Middle parts must exist somewhere after current position
        if let Some(pos) = text[text_pos..].find(part) {
            text_pos += pos + part.len();
        } else {
            return false;
        }
    }

    true
}

/// Execute a single REPL command
async fn execute_command(
    db: &Database,
    cmd: &str,
    args: &[&str],
    current_dir: &Arc<RwLock<Vec<String>>>,
    current_subspace: &Arc<RwLock<String>>,
) -> FdbResult<()> {
    match cmd {
        // ------------- Directory navigation -------------
        "cd" => {
            let new_dir = resolve_path(&current_dir.read().unwrap(), args);

            // Root directory (empty path) always exists, no need to verify
            if new_dir.is_empty() {
                *current_dir.write().unwrap() = new_dir;
                println!("Changed to: /");
                return Ok(());
            }

            // Verify the directory exists by trying to open it
            let trx = db.create_trx()?;
            let path_refs: Vec<&str> = new_dir.iter().map(|s| s.as_str()).collect();
            match dir_open(&trx, &path_refs).await {
                Ok(_) => {
                    let path_display = new_dir.join("/");
                    *current_dir.write().unwrap() = new_dir;
                    println!("Changed to: /{}", path_display);
                }
                Err(e) => {
                    println!("Error: Directory not found: {:?}", e);
                }
            }
            Ok(())
        }

        "pwd" => {
            let current = current_dir.read().unwrap();
            if current.is_empty() {
                println!("/");
            } else {
                println!("/{}", current.join("/"));
            }
            Ok(())
        }

        // ------------- Directory commands -------------
        "dircreate" => {
            if args.is_empty() {
                println!("Usage: dircreate <path...>");
                Ok(())
            } else {
                let trx = db.create_trx()?;
                let resolved = resolve_path(&current_dir.read().unwrap(), args);
                let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
                match dir_create(&trx, &path).await {
                    Ok(_) => {
                        trx.commit().await?;
                        println!("✓ Directory created: /{}", resolved.join("/"));
                    }
                    Err(e) => println!("Error: {:?}", e),
                }
                Ok(())
            }
        }

        "dirlist" | "ls" => {
            let trx = db.create_trx()?;
            let resolved = resolve_path(&current_dir.read().unwrap(), args);
            let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
            match dir_list(&trx, &path).await {
                Ok(children) => {
                    if resolved.is_empty() {
                        println!("Root directories:");
                    } else {
                        println!("Directories under /{}:", resolved.join("/"));
                    }
                    if children.is_empty() {
                        println!("  (none)");
                    } else {
                        for c in children {
                            println!("  {}", c);
                        }
                    }
                }
                Err(e) => println!("Error: {:?}", e),
            }
            Ok(())
        }

        "rmdir" => {
            if args.is_empty() {
                println!("Usage: rmdir <path...>");
                Ok(())
            } else {
                let resolved = resolve_path(&current_dir.read().unwrap(), args);

                // Prevent removing root directory
                if resolved.is_empty() {
                    println!("Error: Cannot remove root directory");
                    return Ok(());
                }

                // Check if the last path component contains a wildcard
                let last_component = resolved.last().unwrap();
                let contains_wildcard = last_component.contains('*');

                if contains_wildcard {
                    // Wildcard pattern matching
                    let pattern = last_component.clone();
                    let parent_path: Vec<String> = resolved[..resolved.len() - 1].to_vec();

                    // List directories at parent level
                    let trx = db.create_trx()?;
                    let parent_path_refs: Vec<&str> =
                        parent_path.iter().map(|s| s.as_str()).collect();

                    match dir_list(&trx, &parent_path_refs).await {
                        Ok(children) => {
                            // Filter directories that match the pattern
                            let matching: Vec<String> = children
                                .iter()
                                .filter(|name| match_pattern(name, &pattern))
                                .cloned()
                                .collect();

                            if matching.is_empty() {
                                println!("No directories matching pattern: {}", pattern);
                                return Ok(());
                            }

                            // Remove each matching directory
                            let mut removed_count = 0;
                            for dir_name in &matching {
                                let mut full_path = parent_path.clone();
                                full_path.push(dir_name.clone());

                                // Check if trying to remove current directory or a parent
                                let current_path_str = current_dir.read().unwrap().join("/");
                                let full_path_str = full_path.join("/");
                                if !current_path_str.is_empty()
                                    && (current_path_str == full_path_str
                                        || current_path_str
                                            .starts_with(&format!("{}/", full_path_str)))
                                {
                                    println!(
                                        "Warning: Removing current directory or its parent. Returning to root."
                                    );
                                    current_dir.write().unwrap().clear();
                                }

                                let path_refs: Vec<&str> =
                                    full_path.iter().map(|s| s.as_str()).collect();
                                match dir_remove(&trx, &path_refs).await {
                                    Ok(true) => {
                                        removed_count += 1;
                                        println!("✓ Directory removed: /{}", full_path.join("/"));
                                    }
                                    Ok(false) => {
                                        println!("Directory not found: /{}", full_path.join("/"));
                                    }
                                    Err(e) => println!("Error removing {}: {:?}", dir_name, e),
                                }
                            }

                            trx.commit().await?;
                            println!(
                                "✓ Removed {} director{} matching pattern: {}",
                                removed_count,
                                if removed_count == 1 { "y" } else { "ies" },
                                pattern
                            );
                        }
                        Err(e) => {
                            println!("Error listing directories: {:?}", e);
                        }
                    }
                } else {
                    // No wildcard - original behavior
                    // Check if trying to remove current directory or a parent
                    let current_path_str = current_dir.read().unwrap().join("/");
                    let resolved_path_str = resolved.join("/");
                    if !current_path_str.is_empty()
                        && (current_path_str == resolved_path_str
                            || current_path_str.starts_with(&format!("{}/", resolved_path_str)))
                    {
                        println!(
                            "Warning: Removing current directory or its parent. Returning to root."
                        );
                        current_dir.write().unwrap().clear();
                    }

                    let trx = db.create_trx()?;
                    let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
                    match dir_remove(&trx, &path).await {
                        Ok(true) => {
                            trx.commit().await?;
                            println!("✓ Directory removed: /{}", resolved.join("/"));
                        }
                        Ok(false) => {
                            println!("Directory not found: /{}", resolved.join("/"));
                        }
                        Err(e) => println!("Error: {:?}", e),
                    }
                }
                Ok(())
            }
        }

        // ------------- Tuple pack/unpack (global, not directory-scoped) -------------
        "keypack" | "pack" => {
            if args.is_empty() {
                println!("Usage: {} (value)", cmd);
                Ok(())
            } else {
                let tuple_str = args.join(" ");
                match tuple_pack_from_string(&tuple_str) {
                    Ok(bytes) => println!("Hex: {}", hex::encode(bytes)),
                    Err(e) => println!("Error: {}", e),
                }
                Ok(())
            }
        }

        "keyunpack" | "unpack" => {
            if let Some(hexkey) = args.get(0) {
                match hex::decode(hexkey) {
                    Ok(bytes) => match tuple_unpack_to_string(&bytes) {
                        Ok(s) => println!("Tuple: {}", s),
                        Err(e) => println!("Error: {}", e),
                    },
                    Err(e) => println!("Invalid hex: {:?}", e),
                }
            } else {
                println!("Usage: {} <hex>", cmd);
            }
            Ok(())
        }

        // ------------- Tuple prefix range / delete -------------
        "clearprefix" => {
            if args.is_empty() {
                println!("Usage: clearprefix [path...] (tuple)");
                Ok(())
            } else {
                let (path_args, tuple_arg) = args.split_at(args.len() - 1);
                let tuple_str = tuple_arg[0].to_string();
                let resolved = resolve_path(&current_dir.read().unwrap(), path_args);
                let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

                let trx = db.create_trx()?;
                let dir = match dir_open(&trx, &path).await {
                    Ok(d) => d,
                    Err(e) => {
                        println!("Error opening dir /{}: {:?}", resolved.join("/"), e);
                        return Ok(());
                    }
                };
                let (begin, end) = match tuple_prefix_range(&dir, &tuple_str) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("Tuple parse error: {}", e);
                        return Ok(());
                    }
                };
                trx.clear_range(&begin, &end);
                trx.commit().await?;
                println!(
                    "✓ Cleared prefix {:?} in /{}",
                    tuple_str,
                    resolved.join("/")
                );

                Ok(())
            }
        }

        "range" => {
            if args.is_empty() {
                println!("Usage: range [path...] (tuple)");
                Ok(())
            } else {
                let (path_args, tuple_arg) = args.split_at(args.len() - 1);
                let tuple_str = tuple_arg[0].to_string();
                let resolved = resolve_path(&current_dir.read().unwrap(), path_args);
                let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

                let trx = db.create_trx()?;
                let dir = match dir_open(&trx, &path).await {
                    Ok(d) => d,
                    Err(e) => {
                        println!("Error opening dir /{}: {:?}", resolved.join("/"), e);
                        return Ok(());
                    }
                };
                let (begin, end) = match tuple_prefix_range(&dir, &tuple_str) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("Tuple parse error: {}", e);
                        return Ok(());
                    }
                };
                let range = RangeOption::from((begin.as_slice(), end.as_slice()));
                let kvs = trx.get_range(&range, 10_000, false).await?;

                println!("Range {:?} in /{}:", tuple_str, resolved.join("/"));
                if kvs.is_empty() {
                    println!("  (no keys)");
                } else {
                    for kv in kvs.iter() {
                        println!(
                            "  key={}, value={}",
                            hex::encode(kv.key()),
                            String::from_utf8_lossy(kv.value())
                        );
                    }
                }
                Ok(())
            }
        }

        "getallvalues" => {
            let resolved = resolve_path(&current_dir.read().unwrap(), args);
            let current_subspace = current_subspace.read().unwrap();
            let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

            let trx = db.create_trx()?;
            let dir = match dir_open(&trx, &path).await {
                Ok(d) => d,
                Err(e) => {
                    println!("Error opening dir /{}: {:?}", resolved.join("/"), e);
                    return Ok(());
                }
            };
            let subspace = if current_subspace.is_empty() {
                None
            } else {
                let subspace = dir
                    .subspace(&current_subspace.as_str())
                    .expect("invalid subspace");
                Some(subspace)
            };

            // Get the full range for this directory
            let (begin, end) = if let Some(subspace) = &subspace {
                subspace.range()
            } else {
                dir.range().expect("dir.range()")
            };
            let range = RangeOption::from((begin.as_slice(), end.as_slice()));
            let kvs = trx.get_range(&range, 10_000, false).await?;

            if resolved.is_empty() {
                println!("All key-value pairs in /:");
            } else {
                println!("All key-value pairs in /{}:", resolved.join("/"));
            }

            if kvs.is_empty() {
                println!("  (no keys)");
            } else {
                for kv in kvs.iter() {
                    let key = if let Some(subspace) = &subspace {
                        subspace.unpack(kv.key()).expect("unpack should succeed")
                    } else {
                        readable_key(kv.key())
                    };
                    let value = readable_key(kv.value());
                    println!("  key={}, value={}", key, value);
                }
            }
            Ok(())
        }

        "delkey" => {
            if args.is_empty() {
                println!("Usage: delkey [path...] (tuple)");
                Ok(())
            } else {
                let (path_args, tuple_arg) = args.split_at(args.len() - 1);
                let tuple_str = tuple_arg[0].to_string();
                let resolved = resolve_path(&current_dir.read().unwrap(), path_args);
                let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

                let trx = db.create_trx()?;
                let dir = match dir_open(&trx, &path).await {
                    Ok(d) => d,
                    Err(e) => {
                        println!("Error opening dir /{}: {:?}", resolved.join("/"), e);
                        return Ok(());
                    }
                };
                let key = match tuple_key_from_string(&dir, &tuple_str) {
                    Ok(k) => k,
                    Err(e) => {
                        println!("Tuple parse error: {}", e);
                        return Ok(());
                    }
                };
                trx.clear(&key);
                trx.commit().await?;
                println!("✓ Deleted key {:?} in /{}", tuple_str, resolved.join("/"));

                Ok(())
            }
        }

        "setkey" => {
            if args.is_empty() {
                println!("Usage: setkey [--subspace name] (tuple) value");
                println!("Note: When using --subspace, the subspace must already exist (create with dircreate)");
                return Ok(());
            }

            // Parse arguments for optional --subspace flag
            let (subspace_name, remaining_args) = if args.get(0).map(|s| *s) == Some("--subspace") {
                if args.len() < 2 {
                    println!("Error: --subspace requires a subspace name");
                    println!("Usage: setkey --subspace name (tuple) value");
                    println!("Note: The subspace must already exist (create with dircreate)");
                    return Ok(());
                }
                (Some(args[1]), &args[2..])
            } else {
                (None, args)
            };

            // Validate we have at least tuple and value
            if remaining_args.len() < 2 {
                println!("Usage: setkey [--subspace name] (tuple) value");
                return Ok(());
            }

            // Split into tuple and value (everything after tuple is the value)
            let tuple_str = remaining_args[0].to_string();
            let value_str = remaining_args[1..].join(" ");

            // Determine the directory path
            let resolved = if let Some(subspace) = subspace_name {
                // Build path: current_dir + subspace_name
                let mut path = current_dir.read().unwrap().clone();
                path.push(subspace.to_string());
                path
            } else {
                // Use current directory
                current_dir.read().unwrap().clone()
            };

            let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

            // Execute the set operation
            let trx = db.create_trx()?;

            // Open the directory (don't create it - subspaces must already exist)
            let dir = match dir_open(&trx, &path).await {
                Ok(d) => d,
                Err(e) => {
                    if subspace_name.is_some() {
                        println!("Error: Subspace '{}' does not exist in /{}. Create it first with: dircreate {}",
                                 subspace_name.unwrap(),
                                 current_dir.read().unwrap().join("/"),
                                 subspace_name.unwrap());
                    } else {
                        println!("Error opening directory /{}: {:?}", resolved.join("/"), e);
                    }
                    return Ok(());
                }
            };

            // Build the key from tuple
            let key = match tuple_key_from_string(&dir, &tuple_str) {
                Ok(k) => k,
                Err(e) => {
                    println!("Tuple parse error: {}", e);
                    return Ok(());
                }
            };

            // Convert value to bytes (UTF-8)
            let value_bytes = value_str.as_bytes();

            // Set the key
            trx.set(&key, value_bytes);

            // Commit transaction
            trx.commit().await?;

            // Success message
            if let Some(subspace) = subspace_name {
                println!(
                    "✓ Set key {} = {:?} in subspace '{}' of /{}",
                    tuple_str,
                    value_str,
                    subspace,
                    current_dir.read().unwrap().join("/")
                );
            } else {
                println!(
                    "✓ Set key {} = {:?} in /{}",
                    tuple_str,
                    value_str,
                    resolved.join("/")
                );
            }

            Ok(())
        }
        "subspaceset" => {
            if let Some(subspace_name) = args.first() {
                *current_subspace.write().unwrap() = subspace_name.to_string();
                Ok(())
            } else {
                println!("Error: subspace name expected");
                Ok(())
            }
        }
        "subspacelist" => {
            let resolved = resolve_path(&current_dir.read().unwrap(), args);
            let path: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();

            let trx = db.create_trx()?;
            let dir = match dir_open(&trx, &path).await {
                Ok(d) => d,
                Err(e) => {
                    println!("Error opening dir /{}: {:?}", resolved.join("/"), e);
                    return Ok(());
                }
            };

            // Get the full range for this directory
            let (begin, end) = dir.range().expect("dir.range()");
            let mut range = RangeOption::from((begin.as_slice(), end.as_slice()));
            range.mode = StreamingMode::WantAll;
            let kvs = trx.get_range(&range, 10_000, true).await?;

            let mut subspaces = std::collections::HashSet::new();

            // Extract subspace names from keys
            for kv in kvs.iter() {
                let key = kv.key();

                // Strip the directory begin prefix
                if key.len() <= begin.len() {
                    continue;
                }
                let remaining = &key[begin.len() - 1..];

                // Look for the pattern: \x02<subspace_name>\x00
                // The subspace name starts after \x02
                if remaining.is_empty() || remaining[0] != 0x02 {
                    continue;
                }

                // Find the first \x00 after \x02
                if let Some(null_pos) = remaining[1..].iter().position(|&b| b == 0x00) {
                    let subspace_name = String::from_utf8_lossy(&remaining[1..1 + null_pos]);
                    subspaces.insert(subspace_name.to_string());
                }
            }

            if resolved.is_empty() {
                println!("Subspaces in /:");
            } else {
                println!("Subspaces in /{}:", resolved.join("/"));
            }

            if subspaces.is_empty() {
                println!("  (none)");
            } else {
                let mut sorted: Vec<_> = subspaces.into_iter().collect();
                sorted.sort();
                for name in sorted {
                    println!("  {}", name);
                }
            }
            Ok(())
        }
        other => {
            println!("Unknown command: {}. Type 'help' for list.", other);
            Ok(())
        }
    }
}

// Tab completion helper for FDB directories
struct FdbDirectoryCompleter<'a> {
    db: &'a Database,
    current_dir: Arc<RwLock<Vec<String>>>,
    runtime_handle: Handle,
}

impl<'a> FdbDirectoryCompleter<'a> {
    fn new(db: &'a Database, current_dir: Arc<RwLock<Vec<String>>>) -> Self {
        Self {
            db,
            current_dir,
            runtime_handle: Handle::current(),
        }
    }

    // Get directory children for a given path
    fn get_directory_children(&self, path: &[String]) -> Vec<String> {
        // Use block_in_place to run async code from within the runtime
        tokio::task::block_in_place(|| {
            self.runtime_handle
                .block_on(async {
                    let trx = self.db.create_trx().ok()?;
                    let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                    dir_list(&trx, &path_refs).await.ok()
                })
                .unwrap_or_default()
        })
    }
}

impl<'a> Completer for FdbDirectoryCompleter<'a> {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Parse the line to extract command and path
        let parts: Vec<&str> = line[..pos].split_whitespace().collect();

        if parts.is_empty() {
            return Ok((0, vec![]));
        }

        let command = parts[0];

        // Only complete for directory commands
        if !matches!(command, "cd" | "ls" | "dirlist" | "dircreate" | "rmdir") {
            return Ok((0, vec![]));
        }

        // Get the path argument (or empty string if none)
        let path_arg = parts.get(1).copied().unwrap_or("");

        // Determine if this is an absolute path
        let is_absolute = path_arg.starts_with('/');

        // Split path into segments
        let segments: Vec<&str> = if path_arg.is_empty() {
            vec![]
        } else {
            path_arg.split('/').collect()
        };

        // Determine parent path and incomplete segment
        let (parent_segments, incomplete): (Vec<&str>, &str) = if is_absolute {
            // Absolute path: /foo/bar/baz -> parent: ["foo", "bar"], incomplete: "baz"
            if segments.len() > 1 {
                let filtered: Vec<&str> = segments[1..segments.len() - 1]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .copied()
                    .collect();
                (filtered, segments.last().unwrap())
            } else if segments.len() == 1 {
                // Just "/", list root
                if segments[0].is_empty() {
                    (vec![], "")
                } else {
                    // "/foo" -> parent: [], incomplete: "foo"
                    (vec![], segments[0])
                }
            } else {
                (vec![], "")
            }
        } else {
            // Relative path: foo/bar/baz -> parent: ["foo", "bar"], incomplete: "baz"
            if segments.is_empty() {
                // Empty path, list current directory
                (vec![], "")
            } else if segments.len() == 1 {
                // Single segment, incomplete is the whole thing
                (vec![], segments[0])
            } else {
                // Multiple segments
                (
                    segments[..segments.len() - 1].to_vec(),
                    segments.last().unwrap(),
                )
            }
        };

        // Build the absolute path to query
        let query_path: Vec<String> = if is_absolute {
            parent_segments.iter().map(|s| s.to_string()).collect()
        } else {
            // Combine current_dir with parent_segments
            let current = self.current_dir.read().unwrap();
            let mut full_path = current.clone();
            full_path.extend(parent_segments.iter().map(|s| s.to_string()));
            full_path
        };

        // Query FDB for children
        let children = self.get_directory_children(&query_path);

        // Filter by prefix and create candidates
        let candidates: Vec<Pair> = children
            .iter()
            .filter(|name| name.starts_with(incomplete))
            .map(|name| Pair {
                display: name.clone(),
                replacement: format!("{}/", name),
            })
            .collect();

        // Calculate start position (where to start replacing)
        let start_pos = if path_arg.is_empty() {
            pos
        } else {
            // Find the start of the incomplete segment
            line[..pos]
                .rfind(incomplete)
                .unwrap_or(pos.saturating_sub(incomplete.len()))
        };

        Ok((start_pos, candidates))
    }
}

// Implement required helper traits
impl<'a> Helper for FdbDirectoryCompleter<'a> {}
impl<'a> Highlighter for FdbDirectoryCompleter<'a> {}
impl<'a> Hinter for FdbDirectoryCompleter<'a> {
    type Hint = String;
}
impl<'a> Validator for FdbDirectoryCompleter<'a> {}

async fn cmd_repl(db: &Database) -> FdbResult<()> {
    // Track current directory (shared with completer)
    let current_dir = Arc::new(RwLock::new(Vec::<String>::new()));
    let current_subspace = Arc::new(RwLock::new(String::new()));

    // Create tab completion helper
    let completer = FdbDirectoryCompleter::new(db, current_dir.clone());

    // Configure editor with completion
    let config = Config::builder()
        .auto_add_history(true)
        .completion_type(CompletionType::List)
        .build();

    // Initialize rustyline editor with completer
    let mut rl = match Editor::with_config(config) {
        Ok(mut editor) => {
            editor.set_helper(Some(completer));
            editor
        }
        Err(e) => {
            eprintln!("Warning: Could not initialize line editor: {}", e);
            eprintln!("History and line editing features will not be available.");
            return cmd_repl_basic(db).await;
        }
    };

    // Load history from file
    let history_path = history_file_path();
    if rl.load_history(&history_path).is_err() {
        // History file doesn't exist yet - this is fine for first run
    }

    println!("fdbcli-next interactive shell");
    println!("Type 'help' for commands, 'quit' or 'exit' to leave.");
    println!("Command history: Use UP/DOWN arrows to navigate.");
    println!("Tab completion: Press TAB to complete directory names.\n");

    loop {
        // Build prompt showing current directory
        let prompt = {
            let current = current_dir.read().unwrap();
            if current.is_empty() {
                "fdbcli-next /> ".to_string()
            } else {
                format!("fdbcli-next /{}> ", current.join("/"))
            }
        };
        let prompt = {
            let current_subspace = current_subspace.read().unwrap();
            if !current_subspace.is_empty() {
                format!("{} ({})", prompt, current_subspace)
            } else {
                prompt
            }
        };

        // Read line with rustyline (provides history, editing, etc.)
        let readline = rl.readline(&prompt);

        match readline {
            Ok(line) => {
                let line = line.trim();

                // Skip empty lines
                if line.is_empty() {
                    continue;
                }

                // Add to history
                if rl.add_history_entry(line).is_err() {
                    // Non-fatal error, just continue
                }

                // Handle quit/exit
                if line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("exit") {
                    break;
                }

                // Handle help
                if line.eq_ignore_ascii_case("help") {
                    print_help();
                    continue;
                }

                // Parse and execute command
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }

                let cmd = parts[0];
                let args = &parts[1..];

                let result = execute_command(db, cmd, args, &current_dir, &current_subspace).await;

                if let Err(e) = result {
                    eprintln!("Error: {:?}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("^C");
                continue; // Don't exit, just cancel current line
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                break;
            }
            Err(err) => {
                eprintln!("Error reading input: {:?}", err);
                break;
            }
        }
    }

    // Save history before exiting
    if let Err(e) = rl.save_history(&history_path) {
        eprintln!("Warning: Could not save command history: {}", e);
    }

    println!("Bye.");
    Ok(())
}

/// Fallback REPL using basic stdin (if rustyline fails)
async fn cmd_repl_basic(db: &Database) -> FdbResult<()> {
    println!("fdbcli-next interactive shell (basic mode)");
    println!("Type 'help' for commands, 'quit' or 'exit' to leave.\n");

    // Track current directory (no completer, but still use Arc<RwLock> for execute_command compatibility)
    let current_dir = Arc::new(RwLock::new(Vec::<String>::new()));
    let current_subspace = Arc::new(RwLock::new(String::new()));

    loop {
        // Build prompt showing current directory
        let prompt = {
            let current = current_dir.read().unwrap();
            if current.is_empty() {
                "fdbcli-next /> ".to_string()
            } else {
                format!("fdbcli-next /{}> ", current.join("/"))
            }
        };
        let prompt = {
            let current_subspace = current_subspace.read().unwrap();
            if !current_subspace.is_empty() {
                format!("{} ({})", prompt, current_subspace)
            } else {
                prompt
            }
        };

        print!("{}", prompt);
        io::stdout().flush().unwrap();

        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err() {
            println!();
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("exit") {
            break;
        }

        if line.eq_ignore_ascii_case("help") {
            print_help();
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let cmd = parts[0];
        let args = &parts[1..];

        let result = execute_command(db, cmd, args, &current_dir, &current_subspace).await;

        if let Err(e) = result {
            eprintln!("Error: {:?}", e);
        }
    }

    println!("Bye.");
    Ok(())
}
