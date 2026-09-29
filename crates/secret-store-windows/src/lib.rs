use nalarvo_application::{CredentialRef, SecretStore, SecretStoreError, SecretValue};

#[cfg(windows)]
mod native {
    use super::*;
    use std::{ffi::c_void, ptr};
    use windows_sys::Win32::{
        Foundation::GetLastError,
        Security::Credentials::{
            CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC, CREDENTIALW, CredDeleteW, CredFree,
            CredReadW, CredWriteW,
        },
    };

    const TARGET_PREFIX: &str = "Nalarvo.SecretStore/";
    const MAX_CREDENTIAL_BYTES: usize = 5_120;
    const ERROR_NOT_FOUND: u32 = 1168;

    fn target(credential: &CredentialRef) -> Result<Vec<u16>, SecretStoreError> {
        if credential.id.trim().is_empty() || credential.id.contains('\0') {
            return Err(SecretStoreError::InvalidReference);
        }
        Ok(format!("{TARGET_PREFIX}{}", credential.id)
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect())
    }

    pub struct WindowsCredentialStore;

    impl SecretStore for WindowsCredentialStore {
        fn get(&self, credential: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
            let target = target(credential)?;
            let mut stored = ptr::null_mut();
            // Credential Manager returns a Windows-owned allocation, always freed below.
            if unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut stored) } == 0 {
                return if unsafe { GetLastError() } == ERROR_NOT_FOUND {
                    Err(SecretStoreError::NotFound)
                } else {
                    Err(SecretStoreError::Backend)
                };
            }
            struct Free(*mut CREDENTIALW);
            impl Drop for Free {
                fn drop(&mut self) {
                    unsafe { CredFree(self.0.cast::<c_void>()) };
                }
            }
            let _free = Free(stored);
            let stored = unsafe { &*stored };
            let size = stored.CredentialBlobSize as usize;
            if size > MAX_CREDENTIAL_BYTES || (size > 0 && stored.CredentialBlob.is_null()) {
                return Err(SecretStoreError::Backend);
            }
            let bytes = if size == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(stored.CredentialBlob, size) }
            };
            Ok(SecretValue::new(bytes))
        }

        fn put(
            &self,
            credential: &CredentialRef,
            secret: &SecretValue,
        ) -> Result<(), SecretStoreError> {
            let target = target(credential)?;
            let bytes = secret.expose();
            if bytes.len() > MAX_CREDENTIAL_BYTES {
                return Err(SecretStoreError::Backend);
            }
            let entry = CREDENTIALW {
                Type: CRED_TYPE_GENERIC,
                TargetName: target.as_ptr() as *mut u16,
                CredentialBlobSize: bytes.len() as u32,
                CredentialBlob: if bytes.is_empty() {
                    ptr::null_mut()
                } else {
                    bytes.as_ptr() as *mut u8
                },
                Persist: CRED_PERSIST_LOCAL_MACHINE,
                ..Default::default()
            };
            if unsafe { CredWriteW(&entry, 0) } == 0 {
                return Err(SecretStoreError::Backend);
            }
            Ok(())
        }

        fn delete(&self, credential: &CredentialRef) -> Result<(), SecretStoreError> {
            let target = target(credential)?;
            if unsafe { CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) } == 0 {
                return if unsafe { GetLastError() } == ERROR_NOT_FOUND {
                    Err(SecretStoreError::NotFound)
                } else {
                    Err(SecretStoreError::Backend)
                };
            }
            Ok(())
        }
    }
}

#[cfg(windows)]
pub use native::WindowsCredentialStore;

#[cfg(not(windows))]
#[derive(Default)]
pub struct WindowsCredentialStore;

#[cfg(not(windows))]
impl SecretStore for WindowsCredentialStore {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        Err(SecretStoreError::Backend)
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::Backend)
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::Backend)
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;

    #[test]
    fn adapter_fails_safely_off_windows() {
        let store = WindowsCredentialStore;
        let reference = CredentialRef::new("credential-1");
        let secret = SecretValue::new("secret-canary-8d3f");
        for error in [
            store.get(&reference).unwrap_err(),
            store.put(&reference, &secret).unwrap_err(),
            store.delete(&reference).unwrap_err(),
        ] {
            assert_eq!(error.to_string(), "Secret store operation failed");
            assert!(!format!("{error:?}").contains("secret-canary-8d3f"));
        }
    }
}
