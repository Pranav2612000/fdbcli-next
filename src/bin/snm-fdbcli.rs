use clap::{Parser, Subcommand};
use foundationdb::api::FdbApiBuilder;
use foundationdb::{Database, FdbResult, RangeOption};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::io::{self, Write};
use std::path::PathBuf;

use snm_fdbcli::{
    connect_db,
    create_spaces,
    open_spaces,
    dump_dir,
    prefix_range_for_user,
    dir_create,
    dir_list,
    dir_open,
    dir_remove,
    tuple_prefix_range,
    tuple_pack_from_string,
    tuple_unpack_to_string,
    tuple_key_from_string,
};

use hex;

/// FoundationDB playground for srotas/*
///
/// Uses env snm_fdbcli_DB_PATH as cluster file path if set.
/// Example:
///   export snm_fdbcli_DB_PATH=/usr/local/etc/foundationdb/fdb.cluster
#[derive(Parser, Debug)]
#[command(name = "snm-fdbcli")]
#[command(about = "FoundationDB Directory/Tuple CLI for Srotas", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create base directories: srotas/users, logins, orders, wallets
    Init,

    /// Seed a sample user (only when you call it)
    Seed {
        /// User id to seed (default: user-1)
        #[arg(short, long, default_value = "user-1")]
        user: String,
    },

    /// Show a user's JSON record
    ShowUser {
        user: String,
    },

    /// Show a user's wallet JSON
    ShowWallet {
        user: String,
    },

    /// Show all logins for a user (tuple range)
    ShowLogins {
        user: String,
    },

    /// Show all orders for a user (tuple range)
    ShowOrders {
        user: String,
    },

    /// Dump all keys in srotas/* (spaces/directories)
    DumpAll,

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
        Commands::Init => cmd_init(&db).await?,
        Commands::Seed { user } => cmd_seed(&db, &user).await?,
        Commands::ShowUser { user } => cmd_show_user(&db, &user).await?,
        Commands::ShowWallet { user } => cmd_show_wallet(&db, &user).await?,
        Commands::ShowLogins { user } => cmd_show_logins(&db, &user).await?,
        Commands::ShowOrders { user } => cmd_show_orders(&db, &user).await?,
        Commands::DumpAll => cmd_dump_all(&db).await?,
        Commands::Repl => cmd_repl(&db).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------
// Srotas-specific commands
// ---------------------------------------------------------------------

async fn cmd_init(db: &Database) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let _ = create_spaces(&trx).await; // just create/open
    trx.commit().await?;
    println!("✓ Directories created: srotas/users, logins, orders, wallets");
    Ok(())
}

async fn cmd_seed(db: &Database, user_id: &str) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (users_dir, logins_dir, orders_dir, wallets_dir) = create_spaces(&trx).await;

    // user
    let user_key = users_dir.pack(&(user_id,)).expect("pack user key");
    let user_val = format!(r#"{{"name":"Alice","id":"{}"}}"#, user_id);
    trx.set(&user_key, user_val.as_bytes());

    // wallet
    let wallet_key = wallets_dir.pack(&(user_id,)).expect("pack wallet key");
    let wallet_val = br#"{"balance":1000,"asset":"USDT"}"#;
    trx.set(&wallet_key, wallet_val);

    // logins
    let login1_key = logins_dir
        .pack(&(user_id, 1_i64))
        .expect("pack login1 key");
    trx.set(&login1_key, b"login from chrome");

    let login2_key = logins_dir
        .pack(&(user_id, 2_i64))
        .expect("pack login2 key");
    trx.set(&login2_key, b"login from mobile");

    // order
    let order_id = "ord-001";
    let order_key = orders_dir
        .pack(&(user_id, order_id))
        .expect("pack order key");
    let order_val = br#"{"side":"BUY","qty":0.1,"asset":"BTCUSDT"}"#;
    trx.set(&order_key, order_val);

    trx.commit().await?;
    println!("✓ Seeded user {}, wallet, 2 logins and 1 order", user_id);
    Ok(())
}

async fn cmd_show_user(db: &Database, user_id: &str) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (users_dir, _, _, _) = open_spaces(&trx).await;

    let user_key = users_dir.pack(&(user_id,)).expect("pack user read key");
    if let Some(v) = trx.get(&user_key, false).await? {
        println!(
            "User {}: {}",
            user_id,
            String::from_utf8_lossy(v.as_ref())
        );
    } else {
        println!("User {} not found", user_id);
    }
    Ok(())
}

async fn cmd_show_wallet(db: &Database, user_id: &str) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (_, _, _, wallets_dir) = open_spaces(&trx).await;

    let wallet_key = wallets_dir
        .pack(&(user_id,))
        .expect("pack wallet read key");
    if let Some(v) = trx.get(&wallet_key, false).await? {
        println!(
            "Wallet of {}: {}",
            user_id,
            String::from_utf8_lossy(v.as_ref())
        );
    } else {
        println!("Wallet for {} not found", user_id);
    }
    Ok(())
}

async fn cmd_show_logins(db: &Database, user_id: &str) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (_, logins_dir, _, _) = open_spaces(&trx).await;

    let (begin, end) = prefix_range_for_user(&logins_dir, user_id);
    let range = RangeOption::from((begin.as_slice(), end.as_slice()));
    let kvs = trx.get_range(&range, 1000, false).await?;

    println!("Logins for user {}:", user_id);
    if kvs.is_empty() {
        println!("  (none)");
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

async fn cmd_show_orders(db: &Database, user_id: &str) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (_, _, orders_dir, _) = open_spaces(&trx).await;

    let (begin, end) = prefix_range_for_user(&orders_dir, user_id);
    let range = RangeOption::from((begin.as_slice(), end.as_slice()));
    let kvs = trx.get_range(&range, 1000, false).await?;

    println!("Orders for user {}:", user_id);
    if kvs.is_empty() {
        println!("  (none)");
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

async fn cmd_dump_all(db: &Database) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let (users_dir, logins_dir, orders_dir, wallets_dir) = open_spaces(&trx).await;

    println!("=== srotas/users ===");
    dump_dir(&trx, &users_dir, 1000).await?;

    println!("\n=== srotas/logins ===");
    dump_dir(&trx, &logins_dir, 1000).await?;

    println!("\n=== srotas/orders ===");
    dump_dir(&trx, &orders_dir, 1000).await?;

    println!("\n=== srotas/wallets ===");
    dump_dir(&trx, &wallets_dir, 1000).await?;

    Ok(())
}

// ---------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------

/// Get the history file path, preferring XDG_DATA_HOME if set
fn history_file_path() -> PathBuf {
    if let Ok(xdg_data) = std::env::var("XDG_DATA_HOME") {
        let mut path = PathBuf::from(xdg_data);
        path.push("snm-fdbcli");
        // Create directory if it doesn't exist
        let _ = std::fs::create_dir_all(&path);
        path.push("history.txt");
        path
    } else if let Ok(home) = std::env::var("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".snm-fdbcli_history");
        path
    } else {
        // Fallback to current directory
        PathBuf::from(".snm-fdbcli_history")
    }
}

/// Print help information
fn print_help() {
    println!(
        "Commands:\n\
         # Srotas helpers\n\
         - init\n\
         - seed <user_id>\n\
         - show-user <user_id>\n\
         - show-wallet <user_id>\n\
         - logins <user_id>\n\
         - orders <user_id>\n\
         - dump-all\n\
         \n\
         # Directory layer (dynamic)\n\
         - cd <path...>               # change current directory\n\
         - cd ..                      # go up one directory\n\
         - cd /                       # go to root directory\n\
         - pwd                        # print current directory\n\
         - dircreate <path...>        # dircreate srotas users\n\
         - dirlist [path...]          # dirlist ; dirlist srotas\n\
         - ls [path...]               # alias for dirlist\n\
         - rmdir <path...>            # remove directory and its contents\n\
         \n\
         # Tuple / key helpers\n\
         - keypack (value)            # prints hex\n\
         - keyunpack <hex>            # prints (value)\n\
         \n\
         # Range & delete by tuple prefix\n\
         - clearprefix <path...> (tuple)\n\
         - range <path...> (tuple)\n\
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
        if *arg == ".." {
            result.pop();
        } else if *arg == "/" {
            result.clear();
        } else {
            result.push(arg.to_string());
        }
    }

    result
}

/// Execute a single REPL command
async fn execute_command(
    db: &Database,
    cmd: &str,
    args: &[&str],
    current_dir: &mut Vec<String>,
) -> FdbResult<()> {
    match cmd {
        // ------------- Directory navigation -------------
        "cd" => {
            let new_dir = resolve_path(current_dir, args);

            // Root directory (empty path) always exists, no need to verify
            if new_dir.is_empty() {
                *current_dir = new_dir;
                println!("Changed to: /");
                return Ok(());
            }

            // Verify the directory exists by trying to open it
            let trx = db.create_trx()?;
            let path_refs: Vec<&str> = new_dir.iter().map(|s| s.as_str()).collect();
            match dir_open(&trx, &path_refs).await {
                Ok(_) => {
                    *current_dir = new_dir;
                    println!("Changed to: /{}", current_dir.join("/"));
                }
                Err(e) => {
                    println!("Error: Directory not found: {:?}", e);
                }
            }
            Ok(())
        }

        "pwd" => {
            if current_dir.is_empty() {
                println!("/");
            } else {
                println!("/{}", current_dir.join("/"));
            }
            Ok(())
        }

        // ------------- Srotas helpers -------------
        "init" => cmd_init(db).await,
        "seed" => {
            let user = args.get(0).copied().unwrap_or("user-1");
            cmd_seed(db, user).await
        }
        "show-user" => {
            if let Some(user) = args.get(0) {
                cmd_show_user(db, user).await
            } else {
                println!("Usage: show-user <user_id>");
                Ok(())
            }
        }
        "show-wallet" => {
            if let Some(user) = args.get(0) {
                cmd_show_wallet(db, user).await
            } else {
                println!("Usage: show-wallet <user_id>");
                Ok(())
            }
        }
        "logins" => {
            if let Some(user) = args.get(0) {
                cmd_show_logins(db, user).await
            } else {
                println!("Usage: logins <user_id>");
                Ok(())
            }
        }
        "orders" => {
            if let Some(user) = args.get(0) {
                cmd_show_orders(db, user).await
            } else {
                println!("Usage: orders <user_id>");
                Ok(())
            }
        }
        "dump-all" => cmd_dump_all(db).await,

        // ------------- Directory commands (dynamic) -------------
        "dircreate" => {
            if args.is_empty() {
                println!("Usage: dircreate <path...>");
                Ok(())
            } else {
                let trx = db.create_trx()?;
                let resolved = resolve_path(current_dir, args);
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
            let resolved = resolve_path(current_dir, args);
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
                let resolved = resolve_path(current_dir, args);

                // Prevent removing root directory
                if resolved.is_empty() {
                    println!("Error: Cannot remove root directory");
                    return Ok(());
                }

                // Check if trying to remove current directory or a parent
                let current_path_str = current_dir.join("/");
                let resolved_path_str = resolved.join("/");
                if !current_path_str.is_empty() &&
                   (current_path_str == resolved_path_str ||
                    current_path_str.starts_with(&format!("{}/", resolved_path_str))) {
                    println!("Warning: Removing current directory or its parent. Returning to root.");
                    current_dir.clear();
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
                let resolved = resolve_path(current_dir, path_args);
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
                println!("✓ Cleared prefix {:?} in /{}", tuple_str, resolved.join("/"));

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
                let resolved = resolve_path(current_dir, path_args);
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

        "delkey" => {
            if args.is_empty() {
                println!("Usage: delkey [path...] (tuple)");
                Ok(())
            } else {
                let (path_args, tuple_arg) = args.split_at(args.len() - 1);
                let tuple_str = tuple_arg[0].to_string();
                let resolved = resolve_path(current_dir, path_args);
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

        other => {
            println!("Unknown command: {}. Type 'help' for list.", other);
            Ok(())
        }
    }
}

async fn cmd_repl(db: &Database) -> FdbResult<()> {
    // Initialize rustyline editor
    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
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

    println!("snm-fdbcli interactive shell");
    println!("Type 'help' for commands, 'quit' or 'exit' to leave.");
    println!("Command history: Use UP/DOWN arrows to navigate.\n");

    // Track current directory
    let mut current_dir: Vec<String> = Vec::new();

    loop {
        // Build prompt showing current directory
        let prompt = if current_dir.is_empty() {
            "snm-fdbcli /> ".to_string()
        } else {
            format!("snm-fdbcli /{}> ", current_dir.join("/"))
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

                let result = execute_command(db, cmd, args, &mut current_dir).await;

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
    println!("snm-fdbcli interactive shell (basic mode)");
    println!("Type 'help' for commands, 'quit' or 'exit' to leave.\n");

    // Track current directory
    let mut current_dir: Vec<String> = Vec::new();

    loop {
        // Build prompt showing current directory
        let prompt = if current_dir.is_empty() {
            "snm-fdbcli /> ".to_string()
        } else {
            format!("snm-fdbcli /{}> ", current_dir.join("/"))
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

        let result = execute_command(db, cmd, args, &mut current_dir).await;

        if let Err(e) = result {
            eprintln!("Error: {:?}", e);
        }
    }

    println!("Bye.");
    Ok(())
}
