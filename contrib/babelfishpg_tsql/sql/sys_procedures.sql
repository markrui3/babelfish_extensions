CREATE PROCEDURE sys.sp_unprepare(IN prep_handle INTEGER) 
AS 'babelfishpg_tsql', 'sp_unprepare'
LANGUAGE C;
GRANT EXECUTE ON PROCEDURE sys.sp_unprepare(IN INTEGER) TO PUBLIC;

CREATE PROCEDURE sys.sp_prepare(INOUT prep_handle INTEGER, IN params varchar(8000),
  		 						IN stmt varchar(8000), IN options int default 1)
AS 'babelfishpg_tsql', 'sp_prepare'
LANGUAGE C;
GRANT EXECUTE ON PROCEDURE sys.sp_prepare(
	INOUT INTEGER, IN varchar(8000), IN varchar(8000), IN int
) TO PUBLIC;

CREATE OR REPLACE FUNCTION sys.sp_getapplock_function (IN "@resource" varchar(255),
                                               IN "@lockmode" varchar(32),
                                               IN "@lockowner" varchar(32) DEFAULT 'TRANSACTION',
                                               IN "@locktimeout" INTEGER DEFAULT -99,
                                               IN "@dbprincipal" varchar(32) DEFAULT 'dbo')
RETURNS INTEGER
AS 'babelfishpg_tsql', 'sp_getapplock_function' LANGUAGE C;
GRANT EXECUTE ON FUNCTION sys.sp_getapplock_function(
	IN varchar(255), IN varchar(32), IN varchar(32), IN INTEGER, IN varchar(32)
) TO PUBLIC;

CREATE OR REPLACE FUNCTION sys.sp_releaseapplock_function(IN "@resource" varchar(255),
                                                   IN "@lockowner" varchar(32) DEFAULT 'TRANSACTION',
                                                   IN "@dbprincipal" varchar(32) DEFAULT 'dbo')
RETURNS INTEGER
AS 'babelfishpg_tsql', 'sp_releaseapplock_function' LANGUAGE C;
GRANT EXECUTE ON FUNCTION sys.sp_releaseapplock_function(
	IN varchar(255), IN varchar(32), IN varchar(32)
) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_cursor_list (INOUT "@cursor_return" refcursor,
                                                IN "@cursor_scope" INTEGER)
AS $$
DECLARE
  cur refcursor;
BEGIN
  IF "@cursor_scope" >= 1 AND "@cursor_scope" <= 3 THEN
    OPEN cur FOR EXECUTE 'SELECT reference_name::name, cursor_name::name, cursor_scope::smallint, status::smallint, model::smallint, concurrency::smallint, scrollable::smallint, open_status::smallint, cursor_rows::numeric(10,0), fetch_status::smallint, column_count::smallint, row_count::numeric(10,0), last_operation::smallint, cursor_handle::int FROM sys.babelfish_cursor_list($1)' USING "@cursor_scope";
  ELSE
    RAISE 'invalid @cursor_scope: %', "@cursor_scope";
  END IF;

  -- PG cursor evaluates the query at first fetch. We need to evaluate table function now because cursor_list() depeneds on "current" tsql_estate().
  -- Running MOVE fowrard and backward to force evaluating sys.babelfish_cursor_list() now.
  MOVE NEXT FROM cur;
  MOVE PRIOR FROM cur;
  SELECT cur INTO "@cursor_return";
END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON PROCEDURE sys.sp_cursor_list(INOUT refcursor, IN INTEGER) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_describe_cursor (INOUT "@cursor_return" refcursor,
                                                   IN "@cursor_source" nvarchar(30),
                                                   IN "@cursor_identity" nvarchar(30))
AS $$
DECLARE
  cur refcursor;
  cursor_source int;
BEGIN
  IF lower("@cursor_source") = 'local' THEN
    cursor_source := 1;
  ELSIF lower("@cursor_source") = 'global' THEN
    cursor_source := 2;
  ELSIF lower("@cursor_source") = 'variable' THEN
    cursor_source := 3;
  ELSE
    RAISE 'invalid @cursor_source: %', "@cursor_source";
  END IF;

  OPEN cur FOR EXECUTE 'SELECT reference_name::name, cursor_name::name, cursor_scope::smallint, status::smallint, model::smallint, concurrency::smallint, scrollable::smallint, open_status::smallint, cursor_rows::numeric(10,0), fetch_status::smallint, column_count::smallint, row_count::numeric(10,0), last_operation::smallint, cursor_handle::int FROM sys.babelfish_cursor_list($1) WHERE cursor_source = $1 and reference_name = $2' USING cursor_source, "@cursor_identity";

  -- PG cursor evaluates the query at first fetch. We need to evaluate table function now because cursor_list() depeneds on "current" tsql_estate().
  -- Running MOVE fowrard and backward to force evaluating sys.babelfish_cursor_list() now.
  MOVE NEXT FROM cur;
  MOVE PRIOR FROM cur;
  SELECT cur INTO "@cursor_return";
END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON PROCEDURE sys.sp_describe_cursor(
	INOUT refcursor, IN nvarchar(30), IN nvarchar(30)
) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_babelfish_configure()
AS 'babelfishpg_tsql', 'sp_babelfish_configure'
LANGUAGE C;
GRANT EXECUTE ON PROCEDURE sys.sp_babelfish_configure() TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_babelfish_configure(IN "@option_name" varchar(128))
AS 'babelfishpg_tsql', 'sp_babelfish_configure'
LANGUAGE C;
GRANT EXECUTE ON PROCEDURE sys.sp_babelfish_configure(IN varchar(128)) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_babelfish_configure(IN "@option_name" varchar(128),  IN "@option_value" varchar(128))
AS $$
BEGIN
  CALL sys.sp_babelfish_configure("@option_name", "@option_value", '');
END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON PROCEDURE sys.sp_babelfish_configure(IN varchar(128), IN varchar(128)) TO PUBLIC;

CREATE VIEW sys.babelfish_configurations_view as
    SELECT * 
    FROM pg_catalog.pg_settings 
    WHERE name collate "C" like 'babelfishpg_tsql.explain_%' OR
          name collate "C" like 'babelfishpg_tsql.escape_hatch_%' OR
          name collate "C" = 'babelfishpg_tsql.enable_pg_hint';
GRANT SELECT on sys.babelfish_configurations_view TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_babelfish_configure(IN "@option_name" varchar(128),  IN "@option_value" varchar(128), IN "@option_scope" varchar(128))
AS $$
DECLARE
  normalized_name varchar(256);
  default_value text;
  value_type text;
  enum_value text[];
  cnt int;
  cur refcursor;
  guc_name varchar(256);
  server boolean := false;
  prev_user text;
BEGIN
  IF lower("@option_name") like 'babelfishpg_tsql.%' collate "C" THEN
    SELECT "@option_name" INTO normalized_name;
  ELSE
    SELECT concat('babelfishpg_tsql.',"@option_name") INTO normalized_name;
  END IF;

  IF lower("@option_scope") = 'server' THEN
    server := true;
  ELSIF btrim("@option_scope") != '' THEN
    RAISE EXCEPTION 'invalid option: %', "@option_scope";
  END IF;

  SELECT COUNT(*) INTO cnt FROM sys.babelfish_configurations_view where name collate "C" like normalized_name;
  IF cnt = 0 THEN 
    RAISE EXCEPTION 'unknown configuration: %', normalized_name;
  ELSIF cnt > 1 AND (lower("@option_value") != 'ignore' AND lower("@option_value") != 'strict' 
                AND lower("@option_value") != 'default') THEN
    RAISE EXCEPTION 'unvalid option: %', lower("@option_value");
  END IF;

  OPEN cur FOR SELECT name FROM sys.babelfish_configurations_view where name collate "C" like normalized_name;
  LOOP
    FETCH NEXT FROM cur into guc_name;
    exit when not found;

    SELECT boot_val, vartype, enumvals INTO default_value, value_type, enum_value FROM pg_catalog.pg_settings WHERE name = guc_name;
    IF lower("@option_value") = 'default' THEN
        PERFORM pg_catalog.set_config(guc_name, default_value, 'false');
    ELSIF lower("@option_value") = 'ignore' or lower("@option_value") = 'strict' THEN
      IF value_type = 'enum' AND enum_value = '{"strict", "ignore"}' THEN
        PERFORM pg_catalog.set_config(guc_name, "@option_value", 'false');
      ELSE
        CONTINUE;
      END IF;
    ELSE
        PERFORM pg_catalog.set_config(guc_name, "@option_value", 'false');
    END IF;
    IF server THEN
      SELECT current_user INTO prev_user;
      PERFORM sys.babelfish_set_role(session_user);
      IF lower("@option_value") = 'default' THEN
        EXECUTE format('ALTER DATABASE %s SET %s = %s', CURRENT_DATABASE(), guc_name, default_value);
      ELSIF lower("@option_value") = 'ignore' or lower("@option_value") = 'strict' THEN
        IF value_type = 'enum' AND enum_value = '{"strict", "ignore"}' THEN
          EXECUTE format('ALTER DATABASE %s SET %s = %s', CURRENT_DATABASE(), guc_name, "@option_value");
        ELSE
          CONTINUE;
        END IF;
      ELSE
        -- store the setting in PG master database so that it can be applied to all bbf databases
        EXECUTE format('ALTER DATABASE %s SET %s = %s', CURRENT_DATABASE(), guc_name, "@option_value");
      END IF;
      PERFORM sys.babelfish_set_role(prev_user);
    END IF;
  END LOOP;

  CLOSE cur;

END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON PROCEDURE sys.sp_babelfish_configure(
	IN varchar(128), IN varchar(128), IN varchar(128)
) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_addrole(IN "@rolename" sys.SYSNAME, IN "@ownername" sys.SYSNAME DEFAULT NULL)
AS 'babelfishpg_tsql', 'sp_addrole' LANGUAGE C;
GRANT EXECUTE on PROCEDURE sys.sp_addrole(IN sys.SYSNAME, IN sys.SYSNAME) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_droprole(IN "@rolename" sys.SYSNAME)
AS 'babelfishpg_tsql', 'sp_droprole' LANGUAGE C;
GRANT EXECUTE on PROCEDURE sys.sp_droprole(IN sys.SYSNAME) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_addrolemember(IN "@rolename" sys.SYSNAME, IN "@membername" sys.SYSNAME)
AS 'babelfishpg_tsql', 'sp_addrolemember' LANGUAGE C;
GRANT EXECUTE on PROCEDURE sys.sp_addrolemember(IN sys.SYSNAME, IN sys.SYSNAME) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_droprolemember(IN "@rolename" sys.SYSNAME, IN "@membername" sys.SYSNAME)
AS 'babelfishpg_tsql', 'sp_droprolemember' LANGUAGE C;
GRANT EXECUTE on PROCEDURE sys.sp_droprolemember(IN sys.SYSNAME, IN sys.SYSNAME) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_addlinkedserver( IN "@server" sys.sysname,
                                                    IN "@srvproduct" sys.nvarchar(128) DEFAULT NULL,
                                                    IN "@provider" sys.nvarchar(128) DEFAULT 'SQLNCLI',
                                                    IN "@datasrc" sys.nvarchar(4000) DEFAULT NULL,
                                                    IN "@location" sys.nvarchar(4000) DEFAULT NULL,
                                                    IN "@provstr" sys.nvarchar(4000) DEFAULT NULL,
                                                    IN "@catalog" sys.sysname DEFAULT NULL)
AS 'babelfishpg_tsql', 'sp_addlinkedserver_internal'
LANGUAGE C;

GRANT EXECUTE ON PROCEDURE sys.sp_addlinkedserver(IN sys.sysname,
                                                  IN sys.nvarchar(128),
                                                  IN sys.nvarchar(128),
                                                  IN sys.nvarchar(4000),
                                                  IN sys.nvarchar(4000),
                                                  IN sys.nvarchar(4000),
                                                  IN sys.sysname)
TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_addlinkedsrvlogin( IN "@rmtsrvname" sys.sysname,
                                                      IN "@useself" sys.varchar(8) DEFAULT 'TRUE',
                                                      IN "@locallogin" sys.sysname DEFAULT NULL,
                                                      IN "@rmtuser" sys.sysname DEFAULT NULL,
                                                      IN "@rmtpassword" sys.sysname DEFAULT NULL)
AS 'babelfishpg_tsql', 'sp_addlinkedsrvlogin_internal'
LANGUAGE C;

GRANT EXECUTE ON PROCEDURE sys.sp_addlinkedsrvlogin(IN sys.sysname,
                                                    IN sys.varchar(8),
                                                    IN sys.sysname,
                                                    IN sys.sysname,
                                                    IN sys.sysname)
TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_droplinkedsrvlogin( IN "@rmtsrvname" sys.sysname,
                                                      IN "@locallogin" sys.sysname)
AS 'babelfishpg_tsql', 'sp_droplinkedsrvlogin_internal'
LANGUAGE C;

GRANT EXECUTE ON PROCEDURE sys.sp_droplinkedsrvlogin(IN sys.sysname,
                                                    IN sys.sysname)
TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_dropserver( IN "@server" sys.sysname,
                                                    IN "@droplogins" sys.bpchar(10) DEFAULT NULL)
AS 'babelfishpg_tsql', 'sp_dropserver_internal'
LANGUAGE C;

GRANT EXECUTE ON PROCEDURE sys.sp_dropserver( IN "@server" sys.sysname,
                                                    IN "@droplogins" sys.bpchar(10))
TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_serveroption( IN "@server" sys.sysname,
                                                    IN "@optname" sys.varchar(35),
                                                    IN "@optvalue" sys.varchar(10))
AS 'babelfishpg_tsql', 'sp_serveroption_internal'
LANGUAGE C;

GRANT EXECUTE ON PROCEDURE sys.sp_serveroption( IN "@server" sys.sysname,
                                                    IN "@optname" sys.varchar(35),
                                                    IN "@optvalue" sys.varchar(10))
TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.sp_babelfish_volatility(IN "@function_name" sys.varchar DEFAULT NULL, IN "@volatility" sys.varchar DEFAULT NULL)
AS 'babelfishpg_tsql', 'sp_babelfish_volatility' LANGUAGE C;
GRANT EXECUTE on PROCEDURE sys.sp_babelfish_volatility(IN sys.varchar, IN sys.varchar) TO PUBLIC;

CREATE OR REPLACE PROCEDURE sys.bbf_set_context_info(IN context_info sys.VARBINARY(128))
AS 'babelfishpg_tsql' LANGUAGE C;

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
