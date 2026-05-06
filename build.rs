fn main() {
    // This assumes the library is in a known location, e.g., /usr/local/lib
    // or defined by an FDB_LIB_DIR environment variable
    println!("{}", format!("-D FDB_API_VERSION=730"));
    #[cfg(target_os = "linux")]
    {
        println!("cargo:rustc-link-search=/usr/lib");
    }
    #[cfg(not(target_os = "linux"))]
    {
        println!("cargo:rustc-link-search=/usr/local/lib");
    }

    println!("cargo:rustc-link-lib=fdb_c");
}
