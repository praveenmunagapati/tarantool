#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(8)

testprefix = "analyzeD"

--!./tcltestrunner.lua
-- 2005 July 22
--
-- The author disclaims copyright to this source code.  In place of
-- a legal notice, here is a blessing:
--
--    May you do good and not evil.
--    May you find forgiveness for yourself and forgive others.
--    May you share freely, never taking more than you give.
--
-------------------------------------------------------------------------
-- This file implements regression tests for SQLite library.
-- This file implements tests for the ANALYZE command.
--
-- Set up a table with the following properties:
--
--    * Contains 1000 rows.
--    * Column a contains even integers between 0 and 18, inclusive (so that
--      a=? for any such integer matches 100 rows).
--    * Column b contains integers between 0 and 9, inclusive.
--    * Column c contains integers between 0 and 199, inclusive (so that
--      for any such integer, c=? matches 5 rows).
--    * Then add 7 rows with a new value for "a" - 3001. The stat4 table will
--      not contain any samples with a=3001.
--
test:do_execsql_test(
    1.0,
    [[
        CREATE TABLE t1(id PRIMARY KEY, a, b, c);
    ]])


test:do_test(
	1.1,
	function()
		for i = 1, 999 do
			local c = math.floor(i % 200);
			test:execsql(string.format(" INSERT INTO t1(id, a, b, c) VALUES(%s, 2*(%s/100), %s%%10, %s ); ", i, i, i, c))
		end
	return test:execsql([[
			INSERT INTO t1 VALUES(1001, 3001, 3001, 3001);
			INSERT INTO t1 VALUES(1002, 3001, 3001, 3002);
			INSERT INTO t1 VALUES(1003, 3001, 3001, 3003);
			INSERT INTO t1 VALUES(1004, 3001, 3001, 3004);
			INSERT INTO t1 VALUES(1005, 3001, 3001, 3005);
			INSERT INTO t1 VALUES(1006, 3001, 3001, 3006);
			INSERT INTO t1 VALUES(1007, 3001, 3001, 3007);
			CREATE INDEX t1_ab ON t1(a, b);
			CREATE INDEX t1_c ON t1(c);
			ANALYZE;
		]])
	end, {
        	-- <1.1> 
        	-- </1.1>
    })

-- With full ANALYZE data, SQLite sees that c=150 (5 rows) is better than
-- a=3001 (7 rows).
--
test:do_execsql_test(
	1.2,
	[[
		EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a=3001 AND c=150;
	]], {
        -- <1.2>
        0, 0, 0, "SEARCH TABLE t1 USING COVERING INDEX t1_c (c=?)"
        -- </1.2>
    })

test:do_test(
	1.3,
	function()
		test:execsql("DELETE FROM _sql_stat1;")
	end, {
        -- <1.3>
        -- </1.3>
    })

-- Without stat1, because 3001 is larger than all samples in the stat4
-- table, SQLite things that a=3001 matches just 1 row. So it (incorrectly)
-- chooses it over the c=150 index (5 rows). Even with stat1 data, things
-- worked this way before commit [e6f7f97dbc].
--
test:do_execsql_test(
	1.4,
	[[
		EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a=3001 AND c=150;
	]], {
        -- <1.4>
        0, 0, 0, "SEARCH TABLE t1 USING COVERING INDEX t1_c (c=?)"
        -- </1.4>
    })

test:do_execsql_test(
	1.5,
	[[
		UPDATE t1 SET a=13 WHERE a = 3001;
		ANALYZE;
	]], {
        -- <1.5> 
        -- </1.5>
    })

test:do_execsql_test(
	1.6,
	[[
		EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a=13 AND c=150;
	]], {
        -- <1.6>
        0, 0, 0, "SEARCH TABLE t1 USING COVERING INDEX t1_c (c=?)"
        -- </1.6>
    })

test:do_test(
	1.7,
	function()
		test:execsql(" DELETE FROM _sql_stat1 ")
	end, {
        -- <1.7> 
        -- </1.7>
   	})

-- Same test as 1.4, except this time the 7 rows that match the a=? condition 
-- do not feature larger values than all rows in the stat4 table. So SQLite
-- gets this right, even without stat1 data.
test:do_execsql_test(
	1.8,
	[[
		EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a=13 AND c=150;
	]], {
        -- <1.8>
        0, 0, 0, "SEARCH TABLE t1 USING COVERING INDEX t1_c (c=?)"
        -- </1.8>
   	})


test:finish_test()
