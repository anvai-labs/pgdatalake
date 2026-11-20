/*--------------------------------------------------------------------------
 *
 * tableam_reloptions.c
 *		Test table access method exercising TableAmRoutine.reloptions.
 *
 * The AM delegates everything to heap, overriding only the reloptions
 * callback, so the regression tests exercise the new hook in isolation.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/tableam_reloptions/tableam_reloptions.c
 *
 *--------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "utils/array.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

/* the one AM-specific option this AM accepts */
#define TEST_OPTION_KEY "test_option"

static bytea *
tableam_reloptions_test_reloptions(Datum reloptions, bool validate)
{
	ArrayType  *array;
	Datum	   *elems;
	int			nelems;
	Datum	   *keep;
	int			nkeep = 0;
	int			i;

	/* no WITH (...) at all: defer to heap defaults */
	if (DatumGetPointer(reloptions) == NULL)
		return heap_reloptions(RELKIND_RELATION, reloptions, validate);

	array = DatumGetArrayTypeP(reloptions);
	deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT,
					 &elems, NULL, &nelems);

	/*
	 * Strip the keys this AM owns, then let heap validate the remainder.
	 * A production AM would instead build its own options struct here
	 * (the build_reloptions pattern index AMs use).
	 */
	keep = (Datum *) palloc(nelems * sizeof(Datum));
	for (i = 0; i < nelems; i++)
	{
		char	   *raw = TextDatumGetCString(elems[i]);
		char	   *eq = strchr(raw, '=');

		if (eq != NULL)
			*eq = '\0';
		if (strcmp(raw, TEST_OPTION_KEY) == 0)
			continue;			/* owned by this AM */
		if (eq != NULL)
			*eq = '=';
		keep[nkeep++] = elems[i];
	}

	array = construct_array_builtin(keep, nkeep, TEXTOID);
	return heap_reloptions(RELKIND_RELATION, PointerGetDatum(array),
						   validate);
}

PG_FUNCTION_INFO_V1(tableam_reloptions_handler);

Datum
tableam_reloptions_handler(PG_FUNCTION_ARGS)
{
	static TableAmRoutine methods;
	HeapTuple	amtup;
	Oid			heap_handler;
	const TableAmRoutine *heap_routine;

	/* fetch heap's routine and delegate everything but reloptions */
	amtup = SearchSysCache1(AMNAME, CStringGetDatum("heap"));
	if (!HeapTupleIsValid(amtup))
		elog(ERROR, "heap access method not found");
	heap_handler = ((Form_pg_am) GETSTRUCT(amtup))->amhandler;
	ReleaseSysCache(amtup);

	heap_routine = GetTableAmRoutine(heap_handler);
	methods = *heap_routine;
	methods.type = T_TableAmRoutine;
	methods.reloptions = tableam_reloptions_test_reloptions;

	PG_RETURN_POINTER(&methods);
}
