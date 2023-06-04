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
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "catalog.h"
#include "extendedproperty.h"
#include "multidb.h"
#include "pltsql.h"
#include "session.h"

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

void
babelfish_exec_extendedproperty(PG_FUNCTION_ARGS, const char *procedure_name)
{
	char		*name,
				*level0type, *level0name,
				*level1type, *level1name,
				*level2type, *level2name;
	bytea		*value;
	int16		db_id;
	char 		*schema_name, *major_name, *minor_name, *type, *var_object_name;
	Oid			schema_id, owner_id;
	Oid			sysadmin, db_owner, cur_user_id;
	bool		has_all_permissions;
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData scanKey[6];
	SysScanDesc scan;

	if (strcmp(procedure_name, "sp_addextendedproperty") == 0 ||
		strcmp(procedure_name, "sp_updateextendedproperty") == 0)
	{
		name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
		value = PG_ARGISNULL(1) ? NULL : PG_GETARG_BYTEA_PP(1);
		level0type = PG_ARGISNULL(2) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(2));
		level0name = PG_ARGISNULL(3) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(3));
		level1type = PG_ARGISNULL(4) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(4));
		level1name = PG_ARGISNULL(5) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(5));
		level2type = PG_ARGISNULL(6) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(6));
		level2name = PG_ARGISNULL(7) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(7));
	}
	else if (strcmp(procedure_name, "sp_dropextendedproperty") == 0)
	{
		name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
		level0type = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(1));
		level0name = PG_ARGISNULL(2) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(2));
		level1type = PG_ARGISNULL(3) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(3));
		level1name = PG_ARGISNULL(4) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(4));
		level2type = PG_ARGISNULL(5) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(5));
		level2name = PG_ARGISNULL(6) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_PP(6));
	}

	sysadmin = get_role_oid("sysadmin", false);
	db_owner = get_role_oid(get_db_owner_name(get_cur_db_name()), false);
	cur_user_id = GetSessionUserId();
	if (is_member_of_role(cur_user_id, sysadmin) ||
		is_member_of_role(cur_user_id, db_owner))
	{
		has_all_permissions = true;
	}

	db_id = get_cur_db_id();
	schema_name = "";
	major_name = "";
	minor_name = "";
	type = "";
	var_object_name = NULL;

	if (!name)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("An invalid parameter or option was specified for procedure '%s'.", procedure_name)));
	}
	remove_trailing_spaces(name);

	if ((!level0type && (level0name || strlen(level0name) == 0)) ||
		(!level0name && (level0type || strlen(level0type) == 0)) ||
		(!level1type && (level1name || strlen(level1name) == 0)) ||
		(!level1name && (level1type || strlen(level1type) == 0)) ||
		(!level2type && (level2name || strlen(level2name) == 0)) ||
		(!level2name && (level2type || strlen(level2type) == 0)) ||
		(level1type && !level0type) ||
		(level2type && (!level0type || !level1type)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("An invalid parameter or option was specified for procedure '%s'.", procedure_name)));
	}

	if (level0type)
	{
		remove_trailing_spaces(level0type);
		level0type = lowerstr(level0type);
	}
	if (level0name)
	{
		remove_trailing_spaces(level0name);
		level0name = lowerstr(level0name);
	}
	if (level1type)
	{
		remove_trailing_spaces(level1type);
		level1type = lowerstr(level1type);
	}
	if (level1name)
	{
		remove_trailing_spaces(level1name);
		level1name = lowerstr(level1name);
	}
	if (level2type)
	{
		remove_trailing_spaces(level2type);
		level2type = lowerstr(level2type);
	}
	if (level2name)
	{
		remove_trailing_spaces(level2name);
		level2name = lowerstr(level2name);
	}

	/* for database */
	if (!level0type)
	{
		if (!has_all_permissions)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Cannot find the object \"object specified\" because it does not exist or you do not have permissions.")));
		}

		type = "DATABASE";
		var_object_name = "object specified";
	}

	/* for schema or object in schema */
	if (strcmp(level0type, "schema") == 0)
	{
		char				*physical_schema_name;
		Form_pg_namespace	nspform;

		var_object_name = level0name;
		schema_name = level0name;
		physical_schema_name = get_physical_schema_name(get_cur_db_name(), schema_name);
		tuple = SearchSysCache1(NAMESPACENAME,
								CStringGetDatum(physical_schema_name));
		pfree(physical_schema_name);

		if (!HeapTupleIsValid(tuple))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
		}

		nspform = (Form_pg_namespace) GETSTRUCT(tuple);
		schema_id = nspform->oid;
		owner_id = nspform->nspowner;
		ReleaseSysCache(tuple);

		if (!level1type)
		{
			if (!has_all_permissions && owner_id != cur_user_id)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Cannot find the object \"%s\" because it does not exist or you do not have permissions.", schema_name)));
			}

			type = "SCHEMA";
			major_name = schema_name;
		}
		else
		{
			Oid reloid;

			if (strcmp(level1type, "table") != 0 &&
				strcmp(level1type, "view") != 0 &&
				strcmp(level1type, "sequence") != 0 &&
				strcmp(level1type, "procedure") != 0 &&
				strcmp(level1type, "function") != 0 &&
				strcmp(level1type, "type") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Extended properties for object type %s are not currently supported in Babelfish.", level1type)));
			}

			var_object_name = psprintf("%s.%s", level0name, level1name);
			major_name = level1name;

			if (strcmp(level1type, "table") == 0 ||
				strcmp(level1type, "view") == 0 ||
				strcmp(level1type, "sequence") == 0)
			{
				Form_pg_class	classform;
				char			relkind;

				tuple = SearchSysCache2(RELNAMENSP,
										CStringGetDatum(major_name),
										Int16GetDatum(schema_id));
				if (!HeapTupleIsValid(tuple))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
				}

				classform = (Form_pg_class) GETSTRUCT(tuple);
				reloid = classform->oid;
				relkind = classform->relkind;
				owner_id = classform->relowner;
				ReleaseSysCache(tuple);

				if ((strcmp(level1type, "table") == 0 && (relkind != RELKIND_RELATION && relkind != RELPERSISTENCE_PERMANENT)) ||
					(strcmp(level1type, "view") == 0 && (relkind != RELKIND_VIEW && relkind != RELKIND_MATVIEW)) ||
					(strcmp(level1type, "sequence") == 0 && relkind != RELKIND_SEQUENCE))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
				}

				if (!has_all_permissions && owner_id != cur_user_id)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Cannot find the object \"%s\" because it does not exist or you do not have permissions.", var_object_name)));
				}
			}
			else if (strcmp(level1type, "procedure") == 0 || strcmp(level1type, "function") == 0)
			{
				CatCList   		*catlist;
				Form_pg_proc	procform;
				bool			find = false;

				catlist = SearchSysCacheList1(PROCNAMEARGSNSP,
											  CStringGetDatum(major_name));
				for (int i = 0; i < catlist->n_members; i++)
				{
					tuple = &catlist->members[i]->tuple;
					procform = (Form_pg_proc) GETSTRUCT(tuple);
					if (procform->pronamespace == schema_id)
					{
						owner_id = procform->proowner;
						find = true;
						break;
					}
				}
				ReleaseSysCacheList(catlist);

				if (!find)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
				}

				if (!has_all_permissions && owner_id != cur_user_id)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Cannot find the object \"%s\" because it does not exist or you do not have permissions.", var_object_name)));
				}
			}
			else if (strcmp(level1type, "type") == 0)
			{
				Form_pg_type	typeform;

				tuple = SearchSysCache2(TYPENAMENSP,
										CStringGetDatum(major_name),
										Int16GetDatum(schema_id));
				if (!HeapTupleIsValid(tuple))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
				}

				typeform = (Form_pg_type) GETSTRUCT(tuple);
				owner_id = typeform->typowner;
				ReleaseSysCache(tuple);

				if (!has_all_permissions && owner_id != cur_user_id)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Cannot find the object \"%s\" because it does not exist or you do not have permissions.", var_object_name)));
				}
			}

			if (!level2type)
			{
				type = level1type;
			}
			else
			{
				if (strcmp(level1type, "table") != 0 &&
					strcmp(level2type, "column") != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Extended properties for object type %s are not currently supported in Babelfish.", level2type)));
				}

				var_object_name = psprintf("%s.%s.%s", level0name, level1name, level2name);
				minor_name = level2name;

				tuple = SearchSysCacheAttName(reloid, minor_name);
				if (!HeapTupleIsValid(tuple))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Object is invalid. Extended properties are not permitted on '%s', or the object does not exist.", var_object_name)));
				}
				ReleaseSysCache(tuple);

				type = psprintf("%s %s", level1type, level2type);
			}
		}
	}

	/* insert/update/drop extended property */
	rel = table_open(get_bbf_extended_properties_oid(), RowExclusiveLock);
	ScanKeyInit(&scanKey[0],
				Anum_bbf_extended_properties_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(db_id));
	ScanKeyInit(&scanKey[1],
				Anum_bbf_extended_properties_schema_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(schema_name));
	ScanKeyInit(&scanKey[2],
				Anum_bbf_extended_properties_major_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(major_name));

	ScanKeyInit(&scanKey[3],
				Anum_bbf_extended_properties_minor_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(minor_name));
	ScanKeyInit(&scanKey[4],
				Anum_bbf_extended_properties_type,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(type));
	ScanKeyInit(&scanKey[5],
				Anum_bbf_extended_properties_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, get_bbf_extended_properties_idx_oid(), true,
							  NULL, 6, scanKey);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		if (strcmp(procedure_name, "sp_addextendedproperty") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Property cannot be added. Property '%s' already exist for '%s'.", name, var_object_name)));
		}
	}
	else
	{
		if (strcmp(procedure_name, "sp_updateextendedproperty") == 0 ||
			strcmp(procedure_name, "sp_dropextendedproperty") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Property cannot be updated or deleted. Property '%s' does not exist for '%s'.", name, var_object_name)));
		}
	}

	if (strcmp(procedure_name, "sp_addextendedproperty") == 0)
	{
		Datum		values[BBF_EXTENDED_PROPERTIES_NUM_COLS];
		bool		nulls[BBF_EXTENDED_PROPERTIES_NUM_COLS];

		MemSet(nulls, false, sizeof(nulls));

		values[0] = Int16GetDatum(db_id);
		values[1] = CStringGetDatum(schema_name);
		values[2] = CStringGetDatum(major_name);
		values[3] = CStringGetDatum(minor_name);
		values[4] = CStringGetTextDatum(type);
		values[5] = CStringGetDatum(name);
		values[6] = CStringGetDatum(value);

		tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	}
	else if (strcmp(procedure_name, "sp_updateextendedproperty") == 0)
	{
		Datum		values[BBF_EXTENDED_PROPERTIES_NUM_COLS];
		bool		nulls[BBF_EXTENDED_PROPERTIES_NUM_COLS];
		bool		replaces[BBF_EXTENDED_PROPERTIES_NUM_COLS];
		HeapTuple	new_tuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));
		values[Anum_bbf_extended_properties_value - 1] = CStringGetDatum(value);
		replaces[Anum_bbf_extended_properties_value - 1] = true;

		new_tuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
									  values, nulls, replaces);
		CatalogTupleUpdate(rel, &new_tuple->t_self, new_tuple);
		heap_freetuple(new_tuple);
	}
	else if (strcmp(procedure_name, "sp_dropextendedproperty") == 0)
	{
		CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	if (level0type)
		pfree(level0type);
	if (level0name)
		pfree(level0name);
	if (level1type)
		pfree(level1type);
	if (level1name)
		pfree(level1name);
	if (level2type)
		pfree(level2type);
	if (level2name)
		pfree(level2name);
}
