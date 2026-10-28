//! `sqlx::migrate!` embeds the migrations directory at compile time and does
//! not notice a new file on its own; a stale build would run tests against
//! an older schema.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
