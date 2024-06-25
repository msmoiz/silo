use silo::Database;

fn main() -> anyhow::Result<()> {
    Database::repl()?;
    Ok(())
}
