//! Resolve purpose-specific credentials from independently approved grants.

use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::time::SystemTime;

use object_store::aws::AwsCredential;
use uuid::Uuid;

use crate::publication_transport::{PublicationCredentials, PublicationPurpose, PublicationTarget};

/// An operator-approved grant from outside the publication ledger. The
/// references are opaque to both the scheduler and the transport.
pub struct PublicationCredentialGrant {
    pub target: PublicationTarget,
    pub purpose: PublicationPurpose,
    pub catalog_credential_ref: String,
    pub object_credential_ref: String,
    pub expires_at: SystemTime,
}

/// Material minted for exactly the approved grant. The credential issuer
/// supplies each expiry independently and enforces provider-side scope.
pub struct PublicationCredentialMaterial {
    pub catalog_bearer: String,
    pub catalog_expires_at: SystemTime,
    pub objects: AwsCredential,
    pub object_expires_at: SystemTime,
}

/// A safe error to propagate to the worker. Provider diagnostics and
/// references stay inside the credential issuer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationCredentialError {
    InvalidGrant,
    DuplicateGrant,
    NotApproved,
    Expired,
    Unavailable,
}

impl fmt::Display for PublicationCredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidGrant => "incomplete publication credential grant",
            Self::DuplicateGrant => "duplicate publication credential grant",
            Self::NotApproved => "publication credentials lack matching approval",
            Self::Expired => "publication credential or grant expired",
            Self::Unavailable => "publication credential issuance unavailable",
        })
    }
}

impl std::error::Error for PublicationCredentialError {}

/// A worker uses a current snapshot of separately provisioned send and lookup
/// grants. Rebuild this from current operator state after rotation or
/// revocation. Every resolution invokes the issuer anew and returns secrets
/// for an individual attempt. The resolver holds only approvals and opaque
/// references and is independent of ledger ownership or send authorization.
pub struct PublicationCredentialResolver {
    grants: HashMap<(Uuid, PublicationPurpose), PublicationCredentialGrant>,
}

impl PublicationCredentialResolver {
    pub fn new(
        approved: impl IntoIterator<Item = PublicationCredentialGrant>,
    ) -> Result<Self, PublicationCredentialError> {
        let mut grants = HashMap::new();
        for grant in approved {
            let binding = grant.target.binding;
            if binding.binding_id.is_nil()
                || binding.authority_id.is_nil()
                || binding.incarnation_id.is_nil()
                || binding.binding_generation <= 0
                || grant.catalog_credential_ref.trim().is_empty()
                || grant.object_credential_ref.trim().is_empty()
            {
                return Err(PublicationCredentialError::InvalidGrant);
            }
            match grants.entry((binding.binding_id, grant.purpose)) {
                Entry::Vacant(entry) => {
                    entry.insert(grant);
                }
                Entry::Occupied(_) => return Err(PublicationCredentialError::DuplicateGrant),
            }
        }
        Ok(Self { grants })
    }

    /// Match the complete SQL descriptor and purpose against independent
    /// approval before calling the trusted issuer. Recheck expiry after
    /// issuance because IO may take longer than the grant or either token.
    pub fn resolve(
        &self,
        target: &PublicationTarget,
        purpose: PublicationPurpose,
        issue: impl FnOnce(
            &PublicationCredentialGrant,
        ) -> Result<PublicationCredentialMaterial, PublicationCredentialError>,
    ) -> Result<PublicationCredentials, PublicationCredentialError> {
        let grant = self
            .grants
            .get(&(target.binding.binding_id, purpose))
            .filter(|grant| &grant.target == target)
            .ok_or(PublicationCredentialError::NotApproved)?;
        if SystemTime::now() >= grant.expires_at {
            return Err(PublicationCredentialError::Expired);
        }

        let material = issue(grant)?;
        let expires_at = grant
            .expires_at
            .min(material.catalog_expires_at)
            .min(material.object_expires_at);
        if SystemTime::now() >= expires_at {
            return Err(PublicationCredentialError::Expired);
        }
        Ok(PublicationCredentials {
            target: grant.target.clone(),
            purpose,
            expires_at,
            catalog_bearer: material.catalog_bearer,
            objects: material.objects,
        })
    }
}

#[cfg(test)]
mod tests;
