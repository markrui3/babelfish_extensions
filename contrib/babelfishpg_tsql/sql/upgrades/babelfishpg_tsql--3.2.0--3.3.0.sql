-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION ""babelfishpg_tsql"" UPDATE TO '3.3.0'" to load this file. \quit

-- add 'sys' to search path for the convenience
SELECT set_config('search_path', 'sys, '||current_setting('search_path'), false);

-- Drops an object if it does not have any dependent objects.
-- Is a temporary procedure for use by the upgrade script. Will be dropped at the end of the upgrade.
-- Please have this be one of the first statements executed in this upgrade script. 
CREATE OR REPLACE PROCEDURE babelfish_drop_deprecated_object(object_type varchar, schema_name varchar, object_name varchar) AS
$$
DECLARE
    error_msg text;
    query1 text;
    query2 text;
BEGIN

    query1 := pg_catalog.format('alter extension babelfishpg_tsql drop %s %s.%s', object_type, schema_name, object_name);
    query2 := pg_catalog.format('drop %s %s.%s', object_type, schema_name, object_name);

    execute query1;
    execute query2;
EXCEPTION
    when object_not_in_prerequisite_state then --if 'alter extension' statement fails
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        raise warning '%', error_msg;
    when dependent_objects_still_exist then --if 'drop view' statement fails
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        raise warning '%', error_msg;
end
$$
LANGUAGE plpgsql;

ALTER FUNCTION sys.parsename(VARCHAR,INT) RENAME TO parsename_deprecated_in_3_3_0;

CREATE OR REPLACE FUNCTION sys.parsename(object_name sys.VARCHAR, object_piece int)
RETURNS sys.SYSNAME
AS 'babelfishpg_tsql', 'parsename'
LANGUAGE C IMMUTABLE STRICT;

CALL sys.babelfish_drop_deprecated_object('function', 'sys', 'parsename_deprecated_in_3_3_0');

CREATE OR REPLACE FUNCTION sys.EOMONTH(date,int DEFAULT 0)
RETURNS date
AS 'babelfishpg_tsql', 'EOMONTH'
LANGUAGE C STABLE PARALLEL SAFE;

ALTER TABLE sys.extended_properties RENAME TO extended_properties_deprecated_in_3_3_0;
CREATE TABLE sys.babelfish_extended_properties (
  dbid smallint NOT NULL,
  schema_name name NOT NULL,
  major_name name NOT NULL,
  minor_name name NOT NULL,
  type sys.varchar(50) NOT NULL,
  name sys.sysname NOT NULL,
  value sys.sql_variant,
  PRIMARY KEY (dbid, schema_name, major_name, minor_name, type, name)
);
GRANT SELECT ON sys.babelfish_extended_properties TO PUBLIC;
GRANT ALL ON TABLE sys.babelfish_extended_properties TO sysadmin;

CREATE OR REPLACE VIEW sys.extended_properties
AS
SELECT class::sys.tinyint, class_desc::sys.nvarchar(60), major_id::int,
  CAST((CASE
    WHEN sub.type = 'DATABASE' THEN 0
    WHEN sub.type = 'SCHEMA' THEN 0
    WHEN sub.type = 'TYPE' THEN 0
    WHEN sub.type IN ('TABLE', 'TABLE COLUMN', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION') THEN (CASE WHEN sub.type = 'TABLE COLUMN' THEN (SELECT attnum FROM pg_attribute WHERE attrelid = sub.major_id AND attname = sub.minor_name COLLATE "C") ELSE 0 END)
  END) AS int) AS minor_id, name::sys.sysname, value::sys.sql_variant
  FROM
  (SELECT
    (CASE
      WHEN type = 'DATABASE' THEN 0
      WHEN type = 'SCHEMA' THEN 3
      WHEN type = 'TYPE' THEN 6
      WHEN type IN ('TABLE', 'TABLE COLUMN', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION') THEN 1
    END) AS class,
    (CASE
      WHEN type = 'DATABASE' THEN 'DATABASE'
      WHEN type = 'SCHEMA' THEN 'SCHEMA'
      WHEN type = 'TYPE' THEN 'TYPE'
      WHEN type IN ('TABLE', 'TABLE COLUMN', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION') THEN 'OBJECT_OR_COLUMN'
    END) AS class_desc,
    (CASE
      WHEN type = 'DATABASE' THEN 0
      WHEN type = 'SCHEMA' THEN sys.schema_id(schema_name::sys.sysname)
      WHEN type = 'TYPE' THEN sys.object_id(schema_name || '.' || major_name)
      WHEN type IN ('TABLE', 'TABLE COLUMN', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION') THEN sys.object_id(schema_name || '.' || major_name)
    END) AS major_id, minor_name, name, value, type
    FROM sys.babelfish_extended_properties WHERE dbid = sys.db_id()) sub ORDER BY class, class_desc, major_id, minor_id, name;
GRANT SELECT ON sys.extended_properties TO PUBLIC;

CALL sys.babelfish_drop_deprecated_object('table', 'sys', 'extended_properties_deprecated_in_3_3_0');

ALTER FUNCTION sys.fn_listextendedproperty RENAME TO fn_listextendedproperty_deprecated_in_3_3_0;
CREATE OR REPLACE FUNCTION sys.fn_listextendedproperty
(
    "@name" sys.sysname,
    "@level0type" VARCHAR(128),
    "@level0name" sys.sysname,
    "@level1type" VARCHAR(128),
    "@level1name" sys.sysname,
    "@level2type" VARCHAR(128),
    "@level2name" sys.sysname
)
RETURNS TABLE (
    objtype sys.sysname,
    objname sys.sysname,
    name sys.sysname,
    value sys.sql_variant
)
AS $$
DECLARE
    var_dbid SMALLINT;
    var_schema_name NAME;
    var_major_name NAME;
    var_minor_name NAME;
    var_type TEXT;
    rec record;
    objtype sys.sysname;
    objname sys.sysname;
BEGIN
    "@level0type" := UPPER(RTRIM("@level0type"));
    "@level1type" := UPPER(RTRIM("@level1type"));
    "@level2type" := UPPER(RTRIM("@level2type"));
    "@level0name" := LOWER(RTRIM("@level0name"));
    "@level1name" := LOWER(RTRIM("@level1name"));
    "@level2name" := LOWER(RTRIM("@level2name"));
    "@name" := RTRIM("@name");

    var_dbid := sys.db_id();
    var_schema_name := '';
    var_major_name := '';
    var_minor_name := '';
    var_type := '';

    -- DATABASE
    IF "@level0type" IS NULL THEN
        var_type := 'DATABASE';
    END IF;

    -- SCHEMA or object in SCHEMA
    IF "@level0type" IN ('SCHEMA') THEN
        var_schema_name := "@level0name";

        -- SCHEMA
        IF "@level1type" IS NULL THEN
            var_type := 'SCHEMA';
            var_major_name := var_schema_name;

        -- object in SCHEMA
        ELSE
            -- if has bigger level type, lower level name should not be NULL, or return empty row.
            IF "@level0name" IS NULL THEN
                RETURN;
            END IF;

            var_major_name := "@level1name";

            IF "@level2type" IS NULL THEN
                var_type := "@level1type";
            ELSE
                -- if has bigger level type, lower level name should not be NULL, or return empty row.
                IF "@level1name" IS NULL THEN
                    RETURN;
                END IF;

                var_type := "@level1type" || ' ' || "@level2type";
                var_minor_name := "@level2name";
            END IF;
        END IF;
    END IF;

    FOR rec IN (SELECT * FROM sys.babelfish_extended_properties ep WHERE ep.dbid = var_dbid AND ep.schema_name = coalesce(var_schema_name, ep.schema_name) COLLATE "C" AND ep.major_name = coalesce(var_major_name, ep.major_name) COLLATE "C" AND ep.minor_name = coalesce(var_minor_name, ep.minor_name) COLLATE "C" AND ep.type = var_type COLLATE "C" AND ep.name = coalesce("@name", ep.name) ORDER BY dbid, schema_name, major_name, minor_name, type, name)
    LOOP
        IF rec.type = 'DATABASE' THEN
            objtype := NULL;
            objname := NULL;
        ELSIF rec.type = 'SCHEMA' THEN
            objtype := 'SCHEMA';
            objname := rec.major_name;
        ELSIF rec.type IN ('TABLE', 'TABLE COLUMN', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION', 'TYPE') THEN
            IF rec.type = 'TABLE COLUMN' THEN
                objtype := 'COLUMN';
                objname := rec.minor_name;
            ELSE
                objtype := rec.type;
                objname := rec.major_name;
            END IF;
        END IF;
        RETURN QUERY SELECT objtype, objname, rec.name AS name, rec.value as value;
    END LOOP;
END;
$$ LANGUAGE plpgsql STABLE;
GRANT EXECUTE ON FUNCTION sys.fn_listextendedproperty TO PUBLIC;

CALL sys.babelfish_drop_deprecated_object('function', 'sys', 'fn_listextendedproperty_deprecated_in_3_3_0');

CREATE OR REPLACE PROCEDURE sys.babelfish_exec_extendedproperty
(
  procedure_name text,
  "@name" sys.sysname,
  "@value" sys.sql_variant,
  "@level0type" VARCHAR(128) = NULL,
  "@level0name" sys.sysname = NULL,
  "@level1type" VARCHAR(128) = NULL,
  "@level1name" sys.sysname = NULL,
  "@level2type" VARCHAR(128) = NULL,
  "@level2name" sys.sysname = NULL
)
AS $$
DECLARE
  var_object_id INT;
  var_user TEXT;
  var_dbid SMALLINT;
  var_schema_name NAME;
  var_major_name NAME;
  var_minor_name NAME;
  var_type TEXT;
  var_object_name TEXT;
  var_object_type TEXT[];
BEGIN
  "@level0type" := UPPER(RTRIM("@level0type"));
  "@level1type" := UPPER(RTRIM("@level1type"));
  "@level2type" := UPPER(RTRIM("@level2type"));
  "@level0name" := LOWER(RTRIM("@level0name"));
  "@level1name" := LOWER(RTRIM("@level1name"));
  "@level2name" := LOWER(RTRIM("@level2name"));
  "@name" := RTRIM("@name");

  var_dbid := sys.db_id();
  var_schema_name := '';
  var_major_name := '';
  var_minor_name := '';
  var_type := '';

  IF "@name" IS NULL THEN
    RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
    RETURN;
  END IF;
  IF ("@level0type" IS NULL AND "@level0name" IS NOT NULL) OR ("@level0type" IS NOT NULL AND "@level0name" IS NULL) THEN
    RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
    RETURN;
  END IF;
  IF ("@level1type" IS NULL AND "@level1name" IS NOT NULL) OR ("@level1type" IS NOT NULL AND "@level1name" IS NULL) THEN
    RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
    RETURN;
  END IF;
  IF ("@level2type" IS NULL AND "@level2name" IS NOT NULL) OR ("@level2type" IS NOT NULL AND "@level2name" IS NULL) THEN
    RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
    RETURN;
  END IF;
  IF "@level1type" IS NOT NULL THEN
    IF "@level0type" IS NULL THEN
      RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
      RETURN;
    END IF;
  END IF;
  IF "@level2type" IS NOT NULL THEN
    IF "@level0type" IS NULL OR "@level1type" IS NULL THEN
      RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
      RETURN;
    END IF;
  END IF;

  -- DATABASE
  IF "@level0type" IS NULL THEN
    IF "@level1type" IS NOT NULL OR "@level2type" IS NOT NULL THEN
      RAISE EXCEPTION 'An invalid parameter or option was specified for procedure ''%''.', procedure_name;
      RETURN;
    END IF;

    var_type := 'DATABASE';
  END IF;

  -- SCHEMA or object in SCHEMA
  IF "@level0type" IN ('SCHEMA') THEN
    IF "@level1type" IS NOT NULL AND "@level1type" NOT IN ('TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION', 'TYPE') THEN
      RAISE EXCEPTION 'Extended properties for object type % are not currently supported by Babelfish.', "@level1type";
      RETURN;
    END IF;
    IF "@level2type" IS NOT NULL THEN
      IF NOT ("@level1type" IN ('TABLE') AND "@level2type" IN ('COLUMN')) THEN
        RAISE EXCEPTION 'Extended properties for object type % are not currently supported by Babelfish.', "@level2type";
        RETURN;
      END IF;
    END IF;

    -- validate SCHEMA
    var_object_name := "@level0name";
    var_schema_name := var_object_name;
    IF sys.schema_id(var_object_name::sys.sysname) IS NULL THEN
      RAISE EXCEPTION 'Object is invalid. Extended properties are not permitted on ''%'', or the object does not exist.', coalesce(var_object_name, 'object specified');
      RETURN;
    END IF;

    -- SCHEMA
    IF "@level1type" IS NULL THEN
      var_type := 'SCHEMA';
      var_major_name := var_schema_name;

    -- object in SCHEMA
    ELSE
      var_major_name := "@level1name";

      -- validate object in SCHEMA
      var_object_name := var_schema_name || '.' || var_major_name;
      IF "@level1type" = 'TABLE' THEN
        var_object_type := ARRAY['IT', 'S', 'U'];
      ELSIF "@level1type" = 'VIEW' THEN
        var_object_type := ARRAY['V'];
      ELSIF "@level1type" = 'SEQUENCE' THEN
        var_object_type := ARRAY['SO'];
      ELSIF "@level1type" = 'PROCEDURE' THEN
        var_object_type := ARRAY['P', 'PC', 'RF', 'X'];
      ELSIF "@level1type" = 'FUNCTION' THEN
        var_object_type := ARRAY['AF', 'FN', 'FS', 'FT', 'IF', 'TF'];
      ELSIF "@level1type" = 'TYPE' THEN
        var_object_type := ARRAY['TT'];
      END IF;
      var_object_id := sys.object_id(var_object_name);
      IF var_object_id IS NULL OR NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = var_object_id AND type = ANY(var_object_type)) THEN
        RAISE EXCEPTION 'Object is invalid. Extended properties are not permitted on ''%'', or the object does not exist.', coalesce(var_object_name, 'object specified');
        RETURN;
      END IF;

      IF "@level2type" IS NULL THEN
        var_type := "@level1type";
      ELSE
        var_type := "@level1type" || ' ' || "@level2type";
        var_minor_name := "@level2name";
        var_object_name := var_schema_name || '.' || var_major_name || '.' || var_minor_name;
        IF "@level1type" IN ('TABLE') AND "@level2type" IN ('COLUMN') AND NOT EXISTS (SELECT * FROM pg_attribute WHERE attrelid = var_object_id AND attname = var_minor_name COLLATE "C") THEN
          RAISE EXCEPTION 'Object is invalid. Extended properties are not permitted on ''%'', or the object does not exist.', coalesce(var_object_name, 'object specified');
          RETURN;
        END IF;
      END IF;
    END IF;
  END IF;

  SELECT current_user INTO var_user;
  PERFORM sys.babelfish_set_role('sysadmin');

  procedure_name = LOWER(procedure_name);
  IF NOT EXISTS (SELECT * FROM sys.babelfish_extended_properties WHERE dbid = var_dbid AND schema_name = var_schema_name COLLATE "C" AND major_name = var_major_name COLLATE "C" AND minor_name = var_minor_name COLLATE "C" AND type = var_type COLLATE "C" AND name = "@name") THEN
    IF procedure_name = 'sp_updateextendedproperty' OR procedure_name = 'sp_dropextendedproperty' THEN
      RAISE EXCEPTION 'Property cannot be updated or deleted. Property ''%'' does not exist for ''%''.', "@name", coalesce(var_object_name, 'object specified');
      RETURN;
    END IF;
  ELSE
    IF procedure_name = 'sp_addextendedproperty' THEN
      RAISE EXCEPTION 'Property cannot be added. Property ''%'' already exists for ''%''.', "@name", coalesce(var_object_name, 'object specified');
    END IF;
  END IF;

  IF procedure_name = 'sp_addextendedproperty' THEN
    INSERT INTO sys.babelfish_extended_properties(dbid, schema_name, major_name, minor_name, type, name, value) VALUES(var_dbid, var_schema_name, var_major_name, var_minor_name, var_type, "@name", "@value");
  ELSIF procedure_name = 'sp_updateextendedproperty' THEN
    UPDATE sys.babelfish_extended_properties SET value = "@value" WHERE dbid = var_dbid AND schema_name = var_schema_name COLLATE "C" AND major_name = var_major_name COLLATE "C" AND minor_name = var_minor_name COLLATE "C" AND type = var_type COLLATE "C" AND name = "@name";
  ELSIF procedure_name = 'sp_dropextendedproperty' THEN
    DELETE FROM sys.babelfish_extended_properties WHERE dbid = var_dbid AND schema_name = var_schema_name COLLATE "C" AND major_name = var_major_name COLLATE "C" AND minor_name = var_minor_name COLLATE "C" AND type = var_type COLLATE "C" AND name = "@name";
  END IF;

  PERFORM sys.babelfish_set_role(var_user);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sys.sp_addextendedproperty
(
  "@name" sys.sysname,
  "@value" sys.sql_variant = NULL,
  "@level0type" VARCHAR(128) = NULL,
  "@level0name" sys.sysname = NULL,
  "@level1type" VARCHAR(128) = NULL,
  "@level1name" sys.sysname = NULL,
  "@level2type" VARCHAR(128) = NULL,
  "@level2name" sys.sysname = NULL
)
AS $$
BEGIN
  CALL sys.babelfish_exec_extendedproperty('sp_addextendedproperty', "@name", "@value", "@level0type", "@level0name", "@level1type", "@level1name", "@level2type", "@level2name");
END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON PROCEDURE sys.sp_addextendedproperty TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_updateextendedproperty
(
  "@name" sys.sysname,
  "@value" sys.sql_variant = NULL,
  "@level0type" VARCHAR(128) = NULL,
  "@level0name" sys.sysname = NULL,
  "@level1type" VARCHAR(128) = NULL,
  "@level1name" sys.sysname = NULL,
  "@level2type" VARCHAR(128) = NULL,
  "@level2name" sys.sysname = NULL
)
LANGUAGE 'plpgsql'
AS $$
BEGIN
  CALL sys.babelfish_exec_extendedproperty('sp_updateextendedproperty', "@name", "@value", "@level0type", "@level0name", "@level1type", "@level1name", "@level2type", "@level2name");
END;
$$;
GRANT EXECUTE ON PROCEDURE sys.sp_updateextendedproperty TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_dropextendedproperty
(
  "@name" sys.sysname,
  "@level0type" VARCHAR(128) = NULL,
  "@level0name" sys.sysname = NULL,
  "@level1type" VARCHAR(128) = NULL,
  "@level1name" sys.sysname = NULL,
  "@level2type" VARCHAR(128) = NULL,
  "@level2name" sys.sysname = NULL
)
LANGUAGE 'plpgsql'
AS $$
BEGIN
  CALL sys.babelfish_exec_extendedproperty('sp_dropextendedproperty', "@name", NULL, "@level0type", "@level0name", "@level1type", "@level1name", "@level2type", "@level2name");
END;
$$;
GRANT EXECUTE ON PROCEDURE sys.sp_dropextendedproperty TO PUBLIC;

-- Drops the temporary procedure used by the upgrade script.
-- Please have this be one of the last statements executed in this upgrade script.
DROP PROCEDURE sys.babelfish_drop_deprecated_object(varchar, varchar, varchar);

-- Reset search_path to not affect any subsequent scripts
SELECT set_config('search_path', trim(leading 'sys, ' from current_setting('search_path')), false);
