/*
 * Native publication credential mappings.
 */
#ifndef MANAGED_CREDENTIALS_H
#define MANAGED_CREDENTIALS_H

#include "pg_datalake_core.h"

extern void lakehouse_load_publication_material(PgDatalakePublicationCredentialMaterial *material,
												Oid role, Oid server_id, Oid mapping_id,
												const char *purpose, MemoryContext context);

#endif
