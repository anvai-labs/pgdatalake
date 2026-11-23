//! Length-delimited credential material shared by the native Send and Lookup boundaries.

use std::time::{Duration, UNIX_EPOCH};

use object_store::aws::AwsCredential;

use crate::publication_transport::{PublicationCredentials, PublicationPurpose, PublicationTarget};

const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;

/// A borrowed UTF-8 byte range. An absent optional value uses NULL and length 0.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgDatalakeCredentialBytes {
    pub data: *const u8,
    pub len: usize,
}

impl PgDatalakeCredentialBytes {
    unsafe fn copy(self) -> Option<String> {
        if self.data.is_null() || !(1..=MAX_CREDENTIAL_BYTES).contains(&self.len) {
            return None;
        }
        let bytes = std::slice::from_raw_parts(self.data, self.len);
        if bytes.contains(&0) {
            return None;
        }
        Some(std::str::from_utf8(bytes).ok()?.to_owned())
    }
}

/// Secret material resolved in one worker for its explicit purpose.
#[repr(C)]
pub struct PgDatalakePublicationCredentialMaterial {
    pub catalog_token: PgDatalakeCredentialBytes,
    pub access_key: PgDatalakeCredentialBytes,
    pub secret_key: PgDatalakeCredentialBytes,
    pub session_token: PgDatalakeCredentialBytes,
    pub expires_unix_seconds: u64,
}

impl PgDatalakePublicationCredentialMaterial {
    /// Caller owns the four byte ranges for this call.
    pub(super) unsafe fn resolve(
        &self,
        target: PublicationTarget,
        purpose: PublicationPurpose,
    ) -> Option<PublicationCredentials> {
        let expires_at = UNIX_EPOCH.checked_add(Duration::from_secs(self.expires_unix_seconds))?;
        let token = if self.session_token.data.is_null() && self.session_token.len == 0 {
            None
        } else {
            Some(self.session_token.copy()?)
        };
        Some(PublicationCredentials {
            target,
            purpose,
            expires_at,
            catalog_bearer: self.catalog_token.copy()?,
            objects: AwsCredential {
                key_id: self.access_key.copy()?,
                secret_key: self.secret_key.copy()?,
                token,
            },
        })
    }
}
