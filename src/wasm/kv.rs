use anyhow::{Context, Result};
use fjall::{Database, Keyspace, PersistMode};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct KvStore {
    database: Arc<Database>,
    keyspace: Arc<Keyspace>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
            keyspace: Arc::clone(&self.keyspace),
        }
    }
}

impl KvStore {
    pub fn new() -> Result<Self> {
        Self::with_path(kv_path())
    }

    pub fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create kv store directory {}", path.display()))?;

        let database = Database::builder(&path)
            .open()
            .with_context(|| format!("failed to open fjall database at {}", path.display()))?;
        let keyspace = database
            .keyspace("plugin_kv", fjall::KeyspaceCreateOptions::default)
            .context("failed to create plugin_kv keyspace")?;

        Ok(Self {
            database: Arc::new(database),
            keyspace: Arc::new(keyspace),
        })
    }

    pub async fn get(&self, scope: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let composite = composite_key(scope, key);
        let keyspace = Arc::clone(&self.keyspace);

        let slice = tokio::task::spawn_blocking(move || keyspace.get(composite)).await??;

        Ok(slice.map(|s| s.to_vec()))
    }

    pub async fn set(&self, scope: String, key: String, value: Vec<u8>) -> Result<()> {
        let composite = composite_key(&scope, &key);
        let keyspace = Arc::clone(&self.keyspace);

        tokio::task::spawn_blocking(move || keyspace.insert(composite, value)).await??;
        Ok(())
    }

    pub async fn save(&self) -> Result<()> {
        let database = Arc::clone(&self.database);
        tokio::task::spawn_blocking(move || database.persist(PersistMode::SyncAll)).await??;
        Ok(())
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new().expect("failed to create default kv store")
    }
}

fn composite_key(scope: &str, key: &str) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(scope.len() + 1 + key.len());
    buffer.extend_from_slice(scope.as_bytes());
    buffer.push(0);
    buffer.extend_from_slice(key.as_bytes());
    buffer
}

pub fn kv_path() -> PathBuf {
    std::env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"))
        .join("plugin-kv")
}
