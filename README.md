# 📦 fdbcli-next
### Next version fdbcli

Based on snm-fdbcli ( https://github.com/srotas-space/snm-fdbcli )
### 🧩 Built on the Official FoundationDB Rust Crate

This project is powered by the official FoundationDB Rust bindings:

```CMD
foundationdb = { version = "0.10.0", features = ["embedded-fdb-include", "fdb-7_3"] }
```


`fdbcli-next` is a powerful **FoundationDB Directory/Tuple explorer**, providing:

- ✔️ CLI commands (`dircreate`, `rmdir`, `dirlist`, `ls`, `cd`, `pack`, `unpack`, `range`, `clearprefix`)  
- ✔️ A REPL / interactive shell (`fdbcli-next repl`)  
- ✔️ High-level Rust APIs for directory management  
- ✔️ Tuple pack/unpack helpers  
- ✔️ Range queries, prefix queries, deletion  
- ✔️ Dump entire subspaces  
- ✔️ Works with any FoundationDB cluster (local or remote)  

---

## 🚀 Features

### Directory Layer  
- Create directories at any depth  
- List subdirectories  
- Open existing directories  

### Tuple Layer  
- Pack `(a, 1, "demo")` into FDB key  
- Unpack bytes back to tuple  
- Automatic prefix-range generation  

### Data Operations  
- Read range  
- Query tuple ranges  
- Delete prefix ranges  
- Dump entire directories  

---

# 🔧 Configuration

Export your cluster file path:

```bash
export FDBCLI_DB_PATH="/usr/local/etc/foundationdb/fdb.cluster"
```

If not set, `Database::default()` is used.

---

# 🐚 REPL MODE

Commands:

```
init
dircreate <path>
dirlist <path>
pack (tuple)
unpack <hex>
range <dir> (tuple)
clearprefix <dir> (tuple)
ls
cd <dir>
rmdir <dir>
help
exit
```

---

# 🔑 Tuple Pack / Unpack

```
pack (user-1, 1)
unpack 01677573...
```

---

# 🧪 Tests

### Unit tests (no DB required)
```bash
cargo test
```

### Full end-to-end (requires running FDB)
```bash
cargo test -- --ignored
```

Inspired by [Srotas Space] (https://srotas.space/open-source) and https://github.com/srotas-space/snm-fdbcli

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
