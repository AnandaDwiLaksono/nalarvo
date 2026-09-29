use nalarvo_application::{
    CredentialRef, FakeSecretStore, SecretStore, SecretStoreError, SecretValue,
};
use serde_json::json;

#[test]
fn credential_ref_serialization_contains_metadata_not_secret() {
    let canary = "secret-canary-8d3f";
    let reference = CredentialRef::new("credential-1");
    let secret = SecretValue::new(canary);
    let serialized = serde_json::to_string(&reference).unwrap();

    assert_eq!(
        json!({"id": "credential-1"}),
        serde_json::from_str::<serde_json::Value>(&serialized).unwrap()
    );
    assert!(!serialized.contains(canary));
    assert!(!format!("{secret:?}").contains(canary));
}

#[test]
fn fake_store_supports_put_get_replace_and_delete() {
    let store = FakeSecretStore::default();
    let reference = CredentialRef::new("credential-1");
    let first = SecretValue::new("first-secret");
    let replacement = SecretValue::new("replacement-secret");

    assert!(matches!(
        store.get(&reference),
        Err(SecretStoreError::NotFound)
    ));
    store.put(&reference, &first).unwrap();
    assert_eq!(store.get(&reference).unwrap().expose(), b"first-secret");
    store.put(&reference, &replacement).unwrap();
    assert_eq!(
        store.get(&reference).unwrap().expose(),
        b"replacement-secret"
    );
    store.delete(&reference).unwrap();
    assert!(matches!(
        store.get(&reference),
        Err(SecretStoreError::NotFound)
    ));
}

#[test]
fn secret_store_errors_are_safe_and_generic() {
    let canary = "secret-canary-8d3f";
    let error = SecretStoreError::Backend;
    assert!(!error.to_string().contains(canary));
    assert!(!format!("{error:?}").contains(canary));
    assert_eq!(error.to_string(), "Secret store operation failed");
}
