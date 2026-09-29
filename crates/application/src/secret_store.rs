use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Durable locator and non-secret credential metadata.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CredentialRef {
    pub id: String,
}

impl CredentialRef {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// Plaintext secret. Deliberately does not implement Serialize or Debug.
pub struct SecretValue(Vec<u8>);

impl SecretValue {
    pub fn new(value: impl AsRef<[u8]>) -> Self {
        Self(value.as_ref().to_vec())
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl Clone for SecretValue {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(REDACTED)")
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecretStoreError {
    #[error("Secret store operation failed")]
    Backend,
    #[error("Secret not found")]
    NotFound,
    #[error("Invalid credential reference")]
    InvalidReference,
}

/// Port for transient plaintext access; implementations own secure persistence.
pub trait SecretStore: Send + Sync {
    fn get(&self, credential: &CredentialRef) -> Result<SecretValue, SecretStoreError>;
    fn put(&self, credential: &CredentialRef, secret: &SecretValue)
    -> Result<(), SecretStoreError>;
    fn delete(&self, credential: &CredentialRef) -> Result<(), SecretStoreError>;
}

#[derive(Clone, Default)]
pub struct FakeSecretStore {
    values: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl SecretStore for FakeSecretStore {
    fn get(&self, credential: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        let values = self.values.lock().map_err(|_| SecretStoreError::Backend)?;
        values
            .get(&credential.id)
            .cloned()
            .map(SecretValue)
            .ok_or(SecretStoreError::NotFound)
    }

    fn put(
        &self,
        credential: &CredentialRef,
        secret: &SecretValue,
    ) -> Result<(), SecretStoreError> {
        if credential.id.trim().is_empty() {
            return Err(SecretStoreError::InvalidReference);
        }
        self.values
            .lock()
            .map_err(|_| SecretStoreError::Backend)?
            .insert(credential.id.clone(), secret.0.clone());
        Ok(())
    }

    fn delete(&self, credential: &CredentialRef) -> Result<(), SecretStoreError> {
        let removed = self
            .values
            .lock()
            .map_err(|_| SecretStoreError::Backend)?
            .remove(&credential.id);
        if removed.is_some() {
            Ok(())
        } else {
            Err(SecretStoreError::NotFound)
        }
    }
}
