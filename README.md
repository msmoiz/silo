# Silo

Silo is an embedded key-value store that persists data to disk in an efficient
manner using a log-structured storage mechanism. It supports fast writes using
an append-only log and uses a hash index in memory to support quick lookups as
well. Log files are compacted on a regular basis to keep disk footprint small,
and entries are checked for data integrity on read using checksums.

You can use it in an application by creating a `Database` and calling methods on
the database object.

```rust
fn main() {
    let mut database = Database::start().unwrap();
    database.set("hello", "world");
    let hello = database.get("hello");
    assert_eq!(hello, "world");
}
```

You can also run an interactive session on the command line:

```shell
> silo
> set hello world
-> set hello
> get hello
-> world
```
