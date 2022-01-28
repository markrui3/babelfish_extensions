DROP VIEW IF EXISTS v_perms_by_name;
GO

DROP TABLE IF EXISTS t_perms_by_name;
GO

CREATE TABLE t_perms_by_name (c1 INT, c2 VARCHAR(16));
GO

SELECT * from sys.HAS_PERMS_BY_NAME('dbo.t_perms_by_name','OBJECT', 'SELECT');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('dbo.t_perms_by_name','OBJECT', 'UPDATE');
GO

-- non-existing table
SELECT * from sys.HAS_PERMS_BY_NAME('dbo.t_perms_by_namet2','OBJECT', 'UPDATE');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('t_perms_by_name','OBJECT', 'SELECT');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('t_perms_by_name','OBJECT', 'UPDATE');
GO

-- non-existing table
SELECT * from sys.HAS_PERMS_BY_NAME('t_perms_by_namet2','OBJECT', 'UPDATE');
GO

-- invalid table spec (three part name or more)
SELECT * from sys.HAS_PERMS_BY_NAME('dbo.t1.t2.abc','OBJECT', 'UPDATE');
GO

CREATE VIEW v_perms_by_name AS
	SELECT * from t_perms_by_name;
GO

SELECT * from sys.HAS_PERMS_BY_NAME('dbo.v_perms_by_name','OBJECT', 'SELECT');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('dbo.v_perms_by_name','OBJECT', 'UPDATE');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('v_perms_by_name','OBJECT', 'SELECT');
GO

SELECT * from sys.HAS_PERMS_BY_NAME('v_perms_by_name','OBJECT', 'UPDATE');
GO

DROP VIEW IF EXISTS v_perms_by_name;
GO

DROP TABLE IF EXISTS t_perms_by_name;
GO

