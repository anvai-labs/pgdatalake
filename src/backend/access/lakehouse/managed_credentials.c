/*
 * managed_credentials.c
 *    Extract bounded material from an operator-owned publication mapping.
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_user_mapping_d.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "utils/acl.h"
#include "utils/memutils.h"

#include "managed_credentials.h"

#define LAKEHOUSE_CREDENTIAL_FDW "lakehouse_publication_credentials"
#define MAX_PUBLICATION_CREDENTIAL_BYTES 4096

typedef enum PublicationCredentialOption
{
	CREDENTIAL_BEARER,
	CREDENTIAL_ACCESS_KEY,
	CREDENTIAL_SECRET_KEY,
	CREDENTIAL_SESSION_TOKEN,
	CREDENTIAL_PURPOSE,
	CREDENTIAL_OPTION_COUNT,
} PublicationCredentialOption;

static const char *const credential_option_names[CREDENTIAL_OPTION_COUNT] = {
	[CREDENTIAL_BEARER] = "token",
	[CREDENTIAL_ACCESS_KEY] = "s3_access_key",
	[CREDENTIAL_SECRET_KEY] = "s3_secret_key",
	[CREDENTIAL_SESSION_TOKEN] = "s3_session_token",
	[CREDENTIAL_PURPOSE] = "purpose",
};

PG_FUNCTION_INFO_V1(pg_lakehouse_validate_publication_credentials);

static void
parse_credential_options(List *options, bool dedicated, const char **values)
{
	ListCell   *cell;

	foreach(cell, options)
	{
		DefElem    *option = lfirst_node(DefElem, cell);
		const char *value;
		int			index;

		for (index = 0; index < CREDENTIAL_OPTION_COUNT; index++)
			if (strcmp(option->defname, credential_option_names[index]) == 0)
				break;
		if (index == CREDENTIAL_OPTION_COUNT)
		{
			if (dedicated)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid publication credential mapping option")));
			continue;
		}
		if (!dedicated && (index == CREDENTIAL_SESSION_TOKEN || index == CREDENTIAL_PURPOSE))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("session credentials require a dedicated publication mapping")));

		value = defGetString(option);
		if (value[0] == '\0' || strlen(value) > MAX_PUBLICATION_CREDENTIAL_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("publication credential option length is outside the supported bounds")));
		values[index] = value;
	}
	if (values[CREDENTIAL_BEARER] == NULL || values[CREDENTIAL_ACCESS_KEY] == NULL ||
		values[CREDENTIAL_SECRET_KEY] == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication requires complete catalog and object credentials")));
	if (dedicated &&
		(values[CREDENTIAL_PURPOSE] == NULL ||
		 (strcmp(values[CREDENTIAL_PURPOSE], "send") != 0 &&
		  strcmp(values[CREDENTIAL_PURPOSE], "lookup") != 0 &&
		  strcmp(values[CREDENTIAL_PURPOSE], "read") != 0)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("publication credential purpose must be send, lookup or read")));
}

Datum
pg_lakehouse_validate_publication_credentials(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	const char *values[CREDENTIAL_OPTION_COUNT] = {0};

	if (catalog == UserMappingRelationId)
		parse_credential_options(options, true, values);
	else if (options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("publication credential options belong on a user mapping")));

	PG_RETURN_VOID();
}

static PgDatalakeCredentialBytes
copy_credential(const char *value, MemoryContext context)
{
	if (value == NULL)
		return (PgDatalakeCredentialBytes)
	{
		0
	};
	return (PgDatalakeCredentialBytes)
	{
		.data = (const uint8 *) MemoryContextStrdup(context, value),
			.len = strlen(value),
	};
}

void
lakehouse_load_publication_material(PgDatalakePublicationCredentialMaterial * material,
									Oid role, Oid server_id, Oid mapping_id,
									const char *purpose, MemoryContext context)
{
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	UserMapping *mapping;
	bool		dedicated;
	const char *values[CREDENTIAL_OPTION_COUNT] = {0};

	if (object_ownercheck(ForeignServerRelationId, server_id, role) ||
		object_aclcheck(ForeignServerRelationId, server_id, role, ACL_USAGE) == ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication requires an operator-owned credential mapping")));

	mapping = GetUserMapping(role, server_id);
	if (mapping->umid != mapping_id || mapping->userid != role)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("managed publication credential mapping no longer matches its approval")));

	server = GetForeignServer(server_id);
	wrapper = GetForeignDataWrapper(server->fdwid);
	dedicated = strcmp(wrapper->fdwname, LAKEHOUSE_CREDENTIAL_FDW) == 0;
	if (dedicated && OidIsValid(wrapper->fdwhandler))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("publication credential server must have no data handler")));
	parse_credential_options(mapping->options, dedicated, values);
	if (dedicated && strcmp(values[CREDENTIAL_PURPOSE], purpose) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("publication credential mapping does not match the worker purpose")));

	material->catalog_token = copy_credential(values[CREDENTIAL_BEARER], context);
	material->access_key = copy_credential(values[CREDENTIAL_ACCESS_KEY], context);
	material->secret_key = copy_credential(values[CREDENTIAL_SECRET_KEY], context);
	material->session_token = copy_credential(values[CREDENTIAL_SESSION_TOKEN], context);
}
