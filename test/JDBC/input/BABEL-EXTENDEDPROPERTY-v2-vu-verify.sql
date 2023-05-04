USE babel_extended_properties_db
GO

-- should fail
EXEC babel_sp_addextendedproperty 'wrong param', 'test', '', NULL
GO

EXEC babel_sp_updateextendedproperty 'wrong param', 'test', NULL, NULL, '', ''
GO

EXEC babel_sp_dropextendedproperty 'wrong param', 'test', '', '', NULL, NULL, '', ''
GO

EXEC babel_sp_updateextendedproperty 'non-existing property', 'test'
GO

EXEC babel_sp_dropextendedproperty 'non-existing property'
GO

EXEC babel_sp_addextendedproperty 'already existing property', 'test'
GO

EXEC babel_sp_addextendedproperty 'already existing property', 'test'
GO

EXEC babel_sp_dropextendedproperty 'wrong param'
GO

EXEC babel_babelfish_exec_extendedproperty 'sp_dropextendedproperty', 'wrong param', NULL
GO

EXEC babel_sp_dropextendedproperty 'already existing property'
GO

SELECT * FROM babel_fn_listextendedproperty(NULL)
GO

SELECT * FROM babel_babelfish_get_extended_properties_view ORDER BY class, class_desc, major_id, minor_id, name
GO

-- database
EXEC babel_extended_properties_proc
GO

EXEC babel_sp_addextendedproperty 'database property1', 'database property1 before'
GO

EXEC babel_sp_addextendedproperty 'database property2', 'database property2 before'
GO

EXEC babel_fn_listextendedproperty NULL, NULL, NULL, NULL, NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'database property1', NULL, NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'database property1', 'database property1 after'
GO

EXEC babel_sp_updateextendedproperty 'database property2', 'database property2 after'
GO

EXEC babel_fn_listextendedproperty NULL, NULL, NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'database property2'
GO

EXEC babel_fn_listextendedproperty NULL, NULL, NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- schema
EXEC babel_extended_properties_proc
GO

CREATE SCHEMA babel_extended_properties_schema1
GO

EXEC babel_sp_addextendedproperty 'schema property1', 'schema property1 before', 'schema', 'babel_extended_properties_schema1'
GO

EXEC babel_sp_addextendedproperty 'schema property2', 'schema property2 before', 'schema', 'babel_extended_properties_schema1'
GO

CREATE SCHEMA babel_extended_properties_schema2
GO

EXEC babel_sp_addextendedproperty 'schema property1', 'schema property1 before', 'schema', 'babel_extended_properties_schema2'
GO

EXEC babel_sp_addextendedproperty 'schema property2', 'schema property2 before', 'schema', 'babel_extended_properties_schema2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, NULL, NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', NULL, NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'schema property1', 'schema', 'babel_extended_properties_schema1', NULL, NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'schema property1', 'schema', NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'schema property1', 'schema property1 after', 'schema', 'babel_extended_properties_schema1'
GO

EXEC babel_sp_updateextendedproperty 'schema property2', 'schema property2 after', 'schema', 'babel_extended_properties_schema2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'schema property2', 'schema', 'babel_extended_properties_schema2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, NULL, NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- table
EXEC babel_extended_properties_proc
GO

CREATE TABLE babel_extended_properties_schema1.babel_extended_properties_table1(id int, name varchar);
GO

EXEC babel_sp_addextendedproperty 'table property1', 'table property1 before', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table1'
GO

EXEC babel_sp_addextendedproperty 'table property2', 'table property2 before', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table1'
GO

CREATE TABLE babel_extended_properties_schema1.babel_extended_properties_table2(id int, name varchar);
GO

EXEC babel_sp_addextendedproperty 'table property1', 'table property1 before', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table2'
GO

EXEC babel_sp_addextendedproperty 'table property2', 'table property2 before', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'table', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'table', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'table property1', 'schema', 'babel_extended_properties_schema1', 'table', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'table property1', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'table property1', 'table property1 after', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table1'
GO

EXEC babel_sp_updateextendedproperty 'table property2', 'table property2 after', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'table', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'table property2', 'schema', 'babel_extended_properties_schema1', 'table', 'babel_extended_properties_table2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'table', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- table column
EXEC babel_extended_properties_proc
GO

EXEC sp_addextendedproperty 'column property1   ', 'column property1 before', 'schema   ', 'babel_extended_properties_schema1   ', 'table   ', 'babel_extended_properties_table1   ', 'column   ', 'id   '
GO

EXEC sp_addextendedproperty 'column property2   ', 'column property2 before', 'schema   ', 'babel_extended_properties_schema1   ', 'table   ', 'babel_extended_properties_table1   ', 'column   ', 'name   '
GO

EXEC sp_addextendedproperty 'column property1   ', 'column property1 before', 'SCHEMA   ', 'BABEL_EXTENDED_PROPERTIES_SCHEMA1   ', 'TABLE   ', 'BABEL_EXTENDED_PROPERTIES_TABLE2   ', 'COLUMN   ', 'NAME   '
GO

EXEC sp_addextendedproperty 'COLUMN PROPERTY2   ', 'COLUMN PROPERTY2 BEFORE   ', 'SCHEMA   ', 'BABEL_EXTENDED_PROPERTIES_SCHEMA1   ', 'TABLE   ', 'BABEL_EXTENDED_PROPERTIES_TABLE2   ', 'COLUMN   ', 'NAME   '
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', NULL, 'Table   ', NULL, 'Column   ', NULL)
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', NULL, 'Column   ', NULL)
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', NULL)
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', 'Id   ')
GO

SELECT * FROM fn_listextendedproperty('Column property1   ', 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', NULL)
GO

SELECT * FROM fn_listextendedproperty('Column property1   ', 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', 'Id   ')
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table2   ', 'Column   ', NULL)
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC sp_updateextendedproperty 'Column property1   ', 'column property1 after', 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table', 'Babel_extended_properties_table1   ', 'Column   ', 'Id   '
GO

EXEC sp_updateextendedproperty 'Column property2   ', 'Column property2 after   ', 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table', 'Babel_extended_properties_table2   ', 'Column   ', 'Name   '
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', NULL)
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC sp_dropextendedproperty 'Column property2   ', 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table2   ', 'Column   ', 'Name   '
GO

SELECT * FROM fn_listextendedproperty(NULL, 'Schema   ', 'Babel_extended_properties_schema1   ', 'Table   ', 'Babel_extended_properties_table1   ', 'Column   ', NULL)
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- view
EXEC babel_extended_properties_proc
GO

CREATE VIEW babel_extended_properties_schema1.babel_extended_properties_view1 AS
SELECT * FROM babel_extended_properties_schema1.babel_extended_properties_table1
GO

EXEC babel_sp_addextendedproperty 'view property1', 'view property1 before', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view1'
GO

EXEC babel_sp_addextendedproperty 'view property2', 'view property2 before', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view1'
GO

CREATE VIEW babel_extended_properties_schema1.babel_extended_properties_view2 AS
SELECT * FROM babel_extended_properties_schema1.babel_extended_properties_table2
GO

EXEC babel_sp_addextendedproperty 'view property1', 'view property1 before', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view2'
GO

EXEC babel_sp_addextendedproperty 'view property2', 'view property2 before', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'view', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'view', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'view property1', 'schema', 'babel_extended_properties_schema1', 'view', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'view property1', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'view property1', 'view property1 after', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view1'
GO

EXEC babel_sp_updateextendedproperty 'view property2', 'view property2 after', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'view', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'view property2', 'schema', 'babel_extended_properties_schema1', 'view', 'babel_extended_properties_view2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'view', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- sequence
EXEC babel_extended_properties_proc
GO

CREATE SEQUENCE babel_extended_properties_schema1.babel_extended_properties_seq1
GO

EXEC babel_sp_addextendedproperty 'sequence property1', 'sequence property1 before', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq1'
GO

EXEC babel_sp_addextendedproperty 'sequence property2', 'sequence property2 before', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq1'
GO

CREATE SEQUENCE babel_extended_properties_schema1.babel_extended_properties_seq2
GO

EXEC babel_sp_addextendedproperty 'sequence property1', 'sequence property1 before', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq2'
GO

EXEC babel_sp_addextendedproperty 'sequence property2', 'sequence property2 before', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'sequence', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'sequence', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'sequence property1', 'schema', 'babel_extended_properties_schema1', 'sequence', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'sequence property1', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'sequence property1', 'sequence property1 after', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq1'
GO

EXEC babel_sp_updateextendedproperty 'sequence property2', 'sequence property2 after', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'sequence', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'sequence property2', 'schema', 'babel_extended_properties_schema1', 'sequence', 'babel_extended_properties_seq2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'sequence', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- procedure
EXEC babel_extended_properties_proc
GO

CREATE PROCEDURE babel_extended_properties_schema1.babel_extended_properties_proc1
AS
BEGIN
    RETURN 1
END
GO

EXEC babel_sp_addextendedproperty 'procedure property1', 'procedure property1 before', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc1'
GO

EXEC babel_sp_addextendedproperty 'procedure property2', 'procedure property2 before', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc1'
GO

CREATE PROCEDURE babel_extended_properties_schema1.babel_extended_properties_proc2
AS
BEGIN
    RETURN 1
END
GO

EXEC babel_sp_addextendedproperty 'procedure property1', 'procedure property1 before', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc2'
GO

EXEC babel_sp_addextendedproperty 'procedure property2', 'procedure property2 before', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'procedure', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'procedure', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'procedure property1', 'schema', 'babel_extended_properties_schema1', 'procedure', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'procedure property1', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'procedure property1', 'procedure property1 after', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc1'
GO

EXEC babel_sp_updateextendedproperty 'procedure property2', 'procedure property2 after', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'procedure', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'procedure property2', 'schema', 'babel_extended_properties_schema1', 'procedure', 'babel_extended_properties_proc2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'procedure', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- function
EXEC babel_extended_properties_proc
GO

CREATE FUNCTION babel_extended_properties_schema1.babel_extended_properties_func1()
RETURNS INT AS
BEGIN
    RETURN 1
END
GO

EXEC babel_sp_addextendedproperty 'function property1', 'function property1 before', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func1'
GO

EXEC babel_sp_addextendedproperty 'function property2', 'function property2 before', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func1'
GO

CREATE FUNCTION babel_extended_properties_schema1.babel_extended_properties_func2()
RETURNS INT AS
BEGIN
    RETURN 1
END
GO

EXEC babel_sp_addextendedproperty 'function property1', 'function property1 before', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func2'
GO

EXEC babel_sp_addextendedproperty 'function property2', 'function property2 before', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'function', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'function', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'function property1', 'schema', 'babel_extended_properties_schema1', 'function', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'function property1', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'function property1', 'function property1 after', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func1'
GO

EXEC babel_sp_updateextendedproperty 'function property2', 'function property2 after', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'function', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'function property2', 'schema', 'babel_extended_properties_schema1', 'function', 'babel_extended_properties_func2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'function', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- type
EXEC babel_extended_properties_proc
GO

CREATE TYPE babel_extended_properties_schema1.babel_extended_properties_type1
AS TABLE(id int)
GO

EXEC babel_sp_addextendedproperty 'type property1', 'type property1 before', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type1'
GO

EXEC babel_sp_addextendedproperty 'type property2', 'type property2 before', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type1'
GO

CREATE TYPE babel_extended_properties_schema1.babel_extended_properties_type2
AS TABLE(id int)
GO

EXEC babel_sp_addextendedproperty 'type property1', 'type property1 before', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type2'
GO

EXEC babel_sp_addextendedproperty 'type property2', 'type property2 before', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', NULL, 'type', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'type', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type1', NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'type property1', 'schema', 'babel_extended_properties_schema1', 'type', NULL, NULL, NULL
GO

EXEC babel_fn_listextendedproperty 'type property1', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type1', NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_updateextendedproperty 'type property1', 'type property1 after', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type1'
GO

EXEC babel_sp_updateextendedproperty 'type property2', 'type property2 after', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'type', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

EXEC babel_sp_dropextendedproperty 'type property2', 'schema', 'babel_extended_properties_schema1', 'type', 'babel_extended_properties_type2'
GO

EXEC babel_fn_listextendedproperty NULL, 'schema', 'babel_extended_properties_schema1', 'type', NULL, NULL, NULL
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- sp_rename
EXEC babel_extended_properties_proc
GO

-- sp_rename type is not supported, so we can only drop babel_extended_properties_type2
-- sp_rename 'babel_extended_properties_schema1.babel_extended_properties_type2', 'babel_extended_properties_type3', 'USERDATATYPE'
-- GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_func2', 'babel_extended_properties_func3', 'OBJECT'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_proc2', 'babel_extended_properties_proc3', 'OBJECT'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_seq2', 'babel_extended_properties_seq3', 'OBJECT'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_view2', 'babel_extended_properties_view3', 'OBJECT'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_table2.name', 'name1', 'COLUMN'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

sp_rename 'babel_extended_properties_schema1.babel_extended_properties_table2', 'babel_extended_properties_table3', 'OBJECT'
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- drop object
EXEC babel_extended_properties_proc
GO

-- sp_rename type failed, so we can only drop babel_extended_properties_type2
DROP TYPE babel_extended_properties_schema1.babel_extended_properties_type2
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP FUNCTION babel_extended_properties_schema1.babel_extended_properties_func3
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP PROCEDURE babel_extended_properties_schema1.babel_extended_properties_proc3
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP SEQUENCE babel_extended_properties_schema1.babel_extended_properties_seq3
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP VIEW babel_extended_properties_schema1.babel_extended_properties_view3
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

-- sp_rename column failed, so we can only drop babel_extended_properties_table3.name
ALTER TABLE babel_extended_properties_schema1.babel_extended_properties_table3 DROP name1
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP TABLE babel_extended_properties_schema1.babel_extended_properties_table3
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

DROP SCHEMA babel_extended_properties_schema2
GO

SELECT * FROM babel_extended_properties_view
GO

EXEC babel_babelfish_extended_properties_proc
GO

USE master
GO

DROP DATABASE babel_extended_properties_db
GO

SELECT class, class_desc, IIF(major_id > 0, 1, 0) AS major_id, minor_id, name, value FROM sys.extended_properties
GO

SELECT dbid, schema_name, major_name, minor_name, type, name, value FROM sys.babelfish_extended_properties ORDER BY dbid, schema_name, major_name, minor_name, type, name;
GO
