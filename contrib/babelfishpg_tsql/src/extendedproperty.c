/*-------------------------------------------------------------------------
 *
 * extendedproperty.c
 *	  support extended property
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "catalog.h"
#include "extendedproperty.h"

void
delete_extended_property(int nkeys,
						 int16 db_id,
						 const char *schema_name,
						 const char *major_name,
						 const char *minor_name,
						 const char *type)
{
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData scanKey[5];
	SysScanDesc scan;

	rel = table_open(get_bbf_extended_properties_oid(), RowExclusiveLock);
	ScanKeyInit(&scanKey[0],
				Anum_bbf_extended_properties_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(db_id));
	if (nkeys > 1)
	{
		ScanKeyInit(&scanKey[1],
                    Anum_bbf_extended_properties_schema_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(schema_name));
	}
	if (nkeys > 2)
	{
		ScanKeyInit(&scanKey[2],
                    Anum_bbf_extended_properties_major_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(major_name));
	}
	if (nkeys > 3)
	{
		ScanKeyInit(&scanKey[3],
                    Anum_bbf_extended_properties_minor_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(minor_name));
	}
	if (nkeys > 4)
	{
		ScanKeyInit(&scanKey[4],
                    Anum_bbf_extended_properties_type,
                    BTEqualStrategyNumber, F_TEXTEQ,
                    CStringGetTextDatum(type));
	}

	scan = systable_beginscan(rel, get_bbf_extended_properties_idx_oid(), true,
							  NULL, nkeys, scanKey);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

void
update_extended_property(int nkeys,
						 int16 db_id,
						 const char *schema_name,
						 const char *major_name,
						 const char *minor_name,
						 const char *type,
						 int attnum,
						 const char *new_value)
{
	Relation	rel;
	HeapTuple	tuple, new_tuple;
	ScanKeyData scanKey[5];
	SysScanDesc scan;
	Datum		values[BBF_EXTENDED_PROPERTIES_NUM_COLS];
	bool		nulls[BBF_EXTENDED_PROPERTIES_NUM_COLS];
	bool		replaces[BBF_EXTENDED_PROPERTIES_NUM_COLS];

	rel = table_open(get_bbf_extended_properties_oid(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0],
				Anum_bbf_extended_properties_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(db_id));
	if (nkeys > 1)
	{
		ScanKeyInit(&scanKey[1],
                    Anum_bbf_extended_properties_schema_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(schema_name));
	}
	if (nkeys > 2)
	{
		ScanKeyInit(&scanKey[2],
                    Anum_bbf_extended_properties_major_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(major_name));
	}
	if (nkeys > 3)
	{
		ScanKeyInit(&scanKey[3],
                    Anum_bbf_extended_properties_minor_name,
                    BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(minor_name));
	}
	if (nkeys > 4)
	{
		ScanKeyInit(&scanKey[4],
                    Anum_bbf_extended_properties_type,
                    BTEqualStrategyNumber, F_TEXTEQ,
                    CStringGetTextDatum(type));
	}

	scan = systable_beginscan(rel, get_bbf_extended_properties_idx_oid(), true,
							  NULL, nkeys, scanKey);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	values[attnum - 1] = CStringGetDatum(new_value);
	replaces[attnum - 1] = true;

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		new_tuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
									  values, nulls, replaces);

		CatalogTupleUpdate(rel, &new_tuple->t_self, new_tuple);

		heap_freetuple(new_tuple);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}