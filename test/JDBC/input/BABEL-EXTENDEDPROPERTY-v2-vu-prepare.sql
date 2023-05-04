-- use a new db
CREATE DATABASE babel_extended_properties_db
GO

USE babel_extended_properties_db
GO

CREATE PROCEDURE babel_babelfish_exec_extendedproperty
    @procedure_name text,
    @name sys.sysname,
    @value sys.sql_variant = NULL,
    @level0type VARCHAR(128) = NULL,
    @level0name sys.sysname = NULL,
    @level1type VARCHAR(128) = NULL,
    @level1name sys.sysname = NULL,
    @level2type VARCHAR(128) = NULL,
    @level2name sys.sysname = NULL
AS
BEGIN
    EXEC babelfish_exec_extendedproperty @procedure_name, @name, @value, @level0type, @level0name, @level1type, @level1name, @level2type, @level2name
END
GO

CREATE PROCEDURE babel_sp_addextendedproperty
    @name sys.sysname,
    @value sys.sql_variant = NULL,
    @level0type VARCHAR(128) = NULL,
    @level0name sys.sysname = NULL,
    @level1type VARCHAR(128) = NULL,
    @level1name sys.sysname = NULL,
    @level2type VARCHAR(128) = NULL,
    @level2name sys.sysname = NULL
AS
BEGIN
    EXEC sp_addextendedproperty @name, @value, @level0type, @level0name, @level1type, @level1name, @level2type, @level2name
END
GO

CREATE PROCEDURE babel_sp_updateextendedproperty
    @name sys.sysname,
    @value sys.sql_variant = NULL,
    @level0type VARCHAR(128) = NULL,
    @level0name sys.sysname = NULL,
    @level1type VARCHAR(128) = NULL,
    @level1name sys.sysname = NULL,
    @level2type VARCHAR(128) = NULL,
    @level2name sys.sysname = NULL
AS
BEGIN
    EXEC sp_updateextendedproperty @name, @value, @level0type, @level0name, @level1type, @level1name, @level2type, @level2name
END
GO

CREATE PROCEDURE babel_sp_dropextendedproperty
    @name sys.sysname,
    @level0type VARCHAR(128) = NULL,
    @level0name sys.sysname = NULL,
    @level1type VARCHAR(128) = NULL,
    @level1name sys.sysname = NULL,
    @level2type VARCHAR(128) = NULL,
    @level2name sys.sysname = NULL
AS
BEGIN
    EXEC sp_dropextendedproperty @name, @level0type, @level0name, @level1type, @level1name, @level2type, @level2name
END
GO

CREATE PROCEDURE babel_fn_listextendedproperty
    @name sys.sysname,
    @level0type VARCHAR(128),
    @level0name sys.sysname,
    @level1type VARCHAR(128),
    @level1name sys.sysname,
    @level2type VARCHAR(128),
    @level2name sys.sysname
AS
BEGIN
    SELECT * FROM fn_listextendedproperty(@name, @level0type, @level0name, @level1type, @level1name, @level2type, @level2name)
END
GO

CREATE VIEW babel_babelfish_get_extended_properties_view AS
SELECT * FROM sys.babelfish_get_extended_properties()
GO

CREATE VIEW babel_extended_properties_view AS
SELECT class, class_desc, IIF(major_id > 0, 1, 0) AS major_id, minor_id, name, value FROM sys.extended_properties
GO

CREATE PROCEDURE babel_extended_properties_proc AS
SELECT class, class_desc, IIF(major_id > 0, 1, 0) AS major_id, minor_id, name, value FROM sys.extended_properties
GO

CREATE PROCEDURE babel_babelfish_extended_properties_proc AS
SELECT IIF(dbid = db_id(), 1, 0) AS dbid, schema_name, major_name, minor_name, type, name, value FROM sys.babelfish_extended_properties ORDER BY dbid, schema_name, major_name, minor_name, type, name
GO
