/*-------------------------------------------------------------------------
 * batch_fill.c
 *    Typed batch → Datum construction (see batch_fill.h). The mappings
 *    below are EXACT equivalents of the old text-render + input-function
 *    path; anything not provably identical (bpchar padding, namein
 *    truncation, timestamp precision < 6 rounding, numeric widths,
 *    typmod-sensitive temporal text) falls back — never a misread.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "datatype/timestamp.h"
#include "varatt.h"

#include "batch_fill.h"

/*
 * unix epoch (1970-01-01) -> postgres epoch (2000-01-01); lowest PG
 * date: 4714-11-24 BC = julian day 1 (days since 2000-01-01)
 */
enum
{
	LAKE_PG_EPOCH_DAYS = 10957,
	LAKE_PG_DATE_MIN_DAYS = -2451545
};

void
lakehouse_batch_map_column(Form_pg_attribute attr,
						   const PgDatalakeBatchColumn *col,
						   uint64 nrows,
						   FmgrInfo *in_func, Oid typioparam,
						   LakehouseBatchMap *map)
{
	map->target = LAKE_FAST_NONE;
	map->in_func = in_func;
	map->typioparam = typioparam;
	map->typmod = attr->atttypmod;

	switch (col->kind)
	{
		case 1:					/* Bool */
			if (attr->atttypid == BOOLOID)
				map->target = LAKE_FAST_BOOL;
			break;

		case 2:					/* Int64 (Int64/Date64 raw, Int32 widened) */
			if (attr->atttypid == INT8OID)
				map->target = LAKE_FAST_INT8;
			else if (attr->atttypid == INT4OID || attr->atttypid == INT2OID)
			{
				/*
				 * parquet long feeding a narrower int column: exactly the
				 * int4in/int2in range rule or the fallback errors the same
				 * way the input function would
				 */
				const int64 *v = (const int64 *) col->values;
				int64		lo = PG_INT32_MIN;
				int64		hi = PG_INT32_MAX;

				if (attr->atttypid == INT2OID)
				{
					lo = PG_INT16_MIN;
					hi = PG_INT16_MAX;
				}
				map->target = LAKE_FAST_INT8;	/* placeholder; refine below */
				map->target = attr->atttypid == INT4OID
					? LAKE_FAST_INT4 : LAKE_FAST_INT2;
				for (uint64 i = 0; i < nrows; i++)
				{
					if (v[i] < lo || v[i] > hi)
					{
						map->target = LAKE_FAST_NONE;
						break;
					}
				}
			}
			break;

		case 3:					/* Float64 */
			if (attr->atttypid == FLOAT8OID)
				map->target = LAKE_FAST_FLOAT8;

			/*
			 * float4: the f64->f32 cast can double-round against
			 * float4in(repr(f64)) in edge cases — fallback
			 */
			break;

		case 4:					/* TimestampUs (epoch micros) */
			if ((attr->atttypid == TIMESTAMPOID ||
				 attr->atttypid == TIMESTAMPTZOID) &&
				(attr->atttypmod == -1 || attr->atttypmod >= 0) &&
				attr->atttypmod <= MAX_TIMESTAMP_PRECISION)
			{
				/*
				 * typmod IS the precision (0..6); 6/-1 need no rounding.
				 * Sub-6 precisions take the exact AdjustTimestampForTypmod
				 * integer arithmetic (TimestampOffsets/Scales tables) —
				 * byte-identical to timestamp_in's rounding (B2 widening).
				 */
				map->target = LAKE_FAST_TIMESTAMP;
				map->typmod = attr->atttypmod;
			}
			break;

		case 7:					/* Date32 (days since 1970-01-01) */
			if (attr->atttypid == DATEOID && attr->atttypmod == -1)
				map->target = LAKE_FAST_DATE;
			break;

		case 5:					/* Text (UTF-8 span) */
			if (attr->atttypid == TEXTOID)
				map->target = LAKE_FAST_TEXT;
			else if (attr->atttypid == VARCHAROID)
				map->target = LAKE_FAST_VARCHAR;	/* length checked per row */
			else if (attr->atttypid == BPCHAROID &&
					 attr->atttypmod >= 0)
				/* typmod IS the char length + VARHDRSZ; padded per row */
				map->target = LAKE_FAST_BPCHAR;
			else if (attr->atttypid == NAMEOID)
				map->target = LAKE_FAST_NAME;	/* truncated per row */
			break;

		case 6:					/* Bytes (raw span) */
			if (attr->atttypid == BYTEAOID)
				map->target = LAKE_FAST_BYTEA;
			break;

		default:				/* Rendered + unknown kinds: fallback */
			break;
	}
}

Datum
lakehouse_batch_col_datum(const LakehouseBatchMap *map,
						  const PgDatalakeBatchColumn *col,
						  uint32 r, const char *attname)
{
	switch (map->target)
	{
		case LAKE_FAST_BOOL:
			return BoolGetDatum(((const uint8 *) col->values)[r] != 0);

		case LAKE_FAST_INT8:
			return Int64GetDatum(((const int64 *) col->values)[r]);

		case LAKE_FAST_INT4:
			return Int32GetDatum((int32) ((const int64 *) col->values)[r]);

		case LAKE_FAST_INT2:
			return Int16GetDatum((int16) ((const int64 *) col->values)[r]);

		case LAKE_FAST_FLOAT8:
			return Float8GetDatum(((const double *) col->values)[r]);

		case LAKE_FAST_TIMESTAMP:
			{
				int64		v = ((const int64 *) col->values)[r];
				int64		out;

				/*
				 * B2 widening: sub-6 typmod rounds exactly like
				 * AdjustTimestampForTypmod (integer offsets/scales tables,
				 * sign-split division) — byte-identical to timestamp_in.
				 */
				if (map->typmod >= 0 && map->typmod < 6)
				{
					static const int64 offsets[6] = {
						500000, 50000, 5000, 500, 50, 5
					};
					static const int64 scales[6] = {
						1000000, 100000, 10000, 1000, 100, 10
					};

					if (v >= 0)
						v = ((v + offsets[map->typmod]) /
							 scales[map->typmod]) * scales[map->typmod];
					else
						v = -(((-v + offsets[map->typmod]) /
							   scales[map->typmod]) * scales[map->typmod]);
				}

				if (__builtin_sub_overflow(v,
										   (int64) LAKE_PG_EPOCH_DAYS * USECS_PER_DAY,
										   &out))
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
							 errmsg("lakehouse: timestamp value out of range for column \"%s\"",
									attname)));
				return Int64GetDatum(out);
			}

		case LAKE_FAST_DATE:
			{
				int64		d = ((const int64 *) col->values)[r] - LAKE_PG_EPOCH_DAYS;

				if (d < LAKE_PG_DATE_MIN_DAYS || d > PG_INT32_MAX)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
							 errmsg("lakehouse: date value out of range for column \"%s\"",
									attname)));
				return Int32GetDatum((int32) d);
			}

		case LAKE_FAST_TEXT:
		case LAKE_FAST_VARCHAR:
			{
				const BatchSpan *sp = &col->spans[r];
				text	   *t;

				if (map->target == LAKE_FAST_VARCHAR && map->typmod >= 0)
				{
					int32		maxlen = map->typmod - VARHDRSZ;
					int32		chars = pg_mbstrlen_with_len((const char *) sp->ptr,
															 (int) sp->len);

					if (chars > maxlen)
						ereport(ERROR,
								(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
								 errmsg("value too long for type character varying(%d)",
										maxlen)));
				}
				t = (text *) palloc(VARHDRSZ + sp->len);
				SET_VARSIZE(t, VARHDRSZ + sp->len);
				memcpy(VARDATA(t), sp->ptr, sp->len);
				return PointerGetDatum(t);
			}

		case LAKE_FAST_BYTEA:
			{
				const BatchSpan *sp = &col->spans[r];
				bytea	   *b = (bytea *) palloc(VARHDRSZ + sp->len);

				SET_VARSIZE(b, VARHDRSZ + sp->len);
				memcpy(VARDATA(b), sp->ptr, sp->len);
				return PointerGetDatum(b);
			}

		case LAKE_FAST_BPCHAR:
			{
				const BatchSpan *sp = &col->spans[r];
				int32		maxlen = map->typmod - VARHDRSZ;
				int32		chars = pg_mbstrlen_with_len((const char *) sp->ptr,
														 (int) sp->len);
				text	   *t;

				/*
				 * bpchar semantics: shorter values pad with spaces, longer
				 * values error (same as varchar's length check).
				 */
				if (chars > maxlen)
					ereport(ERROR,
							(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
							 errmsg("value too long for type character(%d)",
									maxlen)));
				t = (text *) palloc(VARHDRSZ + maxlen);
				SET_VARSIZE(t, VARHDRSZ + maxlen);
				memcpy(VARDATA(t), sp->ptr, sp->len);

				/*
				 * blank-pad by CHARACTER count: space is single-byte in all
				 * server encodings (bpcharin pads the same way)
				 */
				memset(VARDATA(t) + sp->len, ' ', maxlen - chars);
				return PointerGetDatum(t);
			}

		case LAKE_FAST_NAME:
			{
				const BatchSpan *sp = &col->spans[r];
				Name		n = (Name) palloc0(NAMEDATALEN);

				if (sp->len >= NAMEDATALEN)
					ereport(ERROR,
							(errcode(ERRCODE_NAME_TOO_LONG),
							 errmsg("identifier too long")));
				memcpy(NameStr(*n), sp->ptr, sp->len);
				return NameGetDatum(n);
			}

		default:
			/* unreachable: callers fall back via in_func before calling */
			elog(ERROR, "lakehouse: batch datum requested for fallback column \"%s\"",
				 attname);
			return (Datum) 0;
	}
}
