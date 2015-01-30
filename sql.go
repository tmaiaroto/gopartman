/**
 * This file contains all the SQL to load into Postgres all of the functions needed.
 * These are pulled from: https://github.com/keithf4/pg_partman/tree/master/sql/functions
 *
 * Things known:
 *  - Updates are manual (and the SQL isn't exactly verbatim so it can work with Amazon RDS)
 *  - This file is long, but including the SQL here (opposed to external SQL files) means the SQL gets built into the binary making things easier
 */

package main

// Checks if the partition management schema exist in the database.
func (db DB) sqlFunctionsExist() bool {
	var count int
	err := db.Get(&count, "SELECT COUNT(schema_name) FROM information_schema.schemata WHERE schema_name = 'partman';")
	if err != nil {
		log.Error("%v", err)
		return false
	}

	// SELECT count(*) FROM pg_proc WHERE proname = 'create_parent';

	return count > 0
}

// Loads pg_partman functions, types, schema, etc. Call this for each database.
func (db DB) loadPgPartman() {
	// Everything is going under a partman schema.
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS partman;")
	if err != nil {
		log.Error("%v", err)
	}

	db.loadSqlTables()
	db.loadSqlTypes()
	db.loadSqlFunctions()
}

// Removes the partman schema including all objects.
func (db DB) unloadPartman() {
	_, err := db.Exec("DROP SCHEMA IF EXISTS partman CASCADE;")
	if err != nil {
		log.Error("%v", err)
	}
}

// Loads types
func (db DB) loadSqlTypes() {
	_, err := db.Exec(`
		CREATE TYPE partman.check_parent_table AS (parent_table text, count bigint);
	`)
	if err != nil {
		log.Error("%v", err)
	}
}

// Loads functions from pg_partman
func (db DB) loadSqlFunctions() {
	var err error
	tx := db.MustBegin()

	// apply_constraints()
	_, err = tx.Exec(`
		/*
		 * Apply constraints managed by partman extension
		 */
		CREATE FUNCTION partman.apply_constraints(p_parent_table text, p_child_table text DEFAULT NULL, p_analyze boolean DEFAULT TRUE, p_debug boolean DEFAULT FALSE) RETURNS void
		    LANGUAGE plpgsql
		    AS $$
		DECLARE

		v_child_table                   text;
		v_child_tablename               text;
		v_col                           text;
		v_constraint_cols               text[];
		v_constraint_col_type           text;
		v_constraint_name               text;
		v_datetime_string               text;
		v_existing_constraint_name      text;
		v_job_id                        bigint;
		v_jobmon                        boolean;
		v_jobmon_schema                 text;
		v_last_partition                text;
		v_last_partition_id             int; 
		v_last_partition_timestamp      timestamp;
		v_constraint_values             record;
		v_old_search_path               text;
		v_parent_schema                 text;
		v_parent_tablename              text;
		v_part_interval                 text;
		v_partition_suffix              text;
		v_premake                       int;
		v_sql                           text;
		v_step_id                       bigint;
		v_suffix_position               int;
		v_type                          text;

		BEGIN

		SELECT type
		    , part_interval
		    , premake
		    , datetime_string
		    , constraint_cols
		    , jobmon
		INTO v_type
		    , v_part_interval
		    , v_premake
		    , v_datetime_string
		    , v_constraint_cols
		    , v_jobmon
		FROM partman.part_config
		WHERE parent_table = p_parent_table;

		IF v_constraint_cols IS NULL THEN
		    IF p_debug THEN
		        RAISE NOTICE 'Given parent table (%) not set up for constraint management (constraint_cols is NULL)', p_parent_table;
		    END IF;
		    -- Returns silently to allow this function to be simply called by maintenance processes without having to check if config options are set.
		    RETURN;
		END IF;

		SELECT show_partitions INTO v_last_partition FROM partman.show_partitions(p_parent_table, 'DESC') LIMIT 1;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN CREATE CONSTRAINT: '||p_parent_table);
		END IF;

		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;

		-- If p_child_table is null, figure out the partition that is the one right before the premake value backwards.
		IF p_child_table IS NULL THEN
		    
		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Automatically determining most recent child on which to apply constraints');
		    END IF;

		    v_suffix_position := (length(v_last_partition) - position('p_' in reverse(v_last_partition))) + 2;

		    IF v_type IN ('time-static', 'time-dynamic') THEN
		        v_last_partition_timestamp := to_timestamp(substring(v_last_partition from v_suffix_position), v_datetime_string);
		        v_partition_suffix := to_char(v_last_partition_timestamp - (v_part_interval::interval * ((v_premake * 2)+1) ), v_datetime_string);
		    ELSIF v_type IN ('id-static', 'id-dynamic') THEN
		        v_last_partition_id := substring(v_last_partition from v_suffix_position)::int;
		        v_partition_suffix := (v_last_partition_id - (v_part_interval::int * ((v_premake * 2)+1) ))::text; 
		    END IF;
		    
		    v_child_table := partman.check_name_length(v_parent_tablename, v_parent_schema, v_partition_suffix, TRUE);

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Target child table: '||v_child_table);
		    END IF;
		ELSE
		    v_child_table := p_child_table;
		END IF;
		    
		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Checking if target child table exists');
		END IF;

		SELECT tablename INTO v_child_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_child_table;
		IF v_child_tablename IS NULL THEN
		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'NOTICE', 'Target child table ('||v_child_table||') does not exist. Skipping constraint creation.');
		        PERFORM close_job(v_job_id);
		        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		    END IF;
		    IF p_debug THEN
		        RAISE NOTICE 'Target child table (%) does not exist. Skipping constraint creation.', v_child_table;
		    END IF;
		    RETURN;
		ELSE
		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;
		END IF;

		FOREACH v_col IN ARRAY v_constraint_cols
		LOOP
		    SELECT c.conname
		    INTO v_existing_constraint_name
		    FROM pg_catalog.pg_constraint c 
		        JOIN pg_catalog.pg_attribute a ON c.conrelid = a.attrelid 
		    WHERE conrelid = v_child_table::regclass 
		        AND c.conname LIKE 'partmanconstr_%'
		        AND c.contype = 'c' 
		        AND a.attname = v_col
		        AND ARRAY[a.attnum] <@ c.conkey 
		        AND a.attisdropped = false;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Applying new constraint on column: '||v_col);
		    END IF;

		    IF v_existing_constraint_name IS NOT NULL THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'NOTICE', 'Partman managed constraint already exists on this table ('||v_child_table||') and column ('||v_col||'). Skipping creation.');
		        END IF;
		        RAISE WARNING 'Partman managed constraint already exists on this table (%) and column (%). Skipping creation.', v_child_table, v_col ;
		        CONTINUE;
		    END IF;

		    -- Ensure column name gets put on end of constraint name to help avoid naming conflicts 
		    v_constraint_name := partman.check_name_length('partmanconstr_'||v_child_tablename, p_suffix := '_'||v_col);

		    EXECUTE 'SELECT min('||v_col||')::text AS min, max('||v_col||')::text AS max FROM '||v_child_table INTO v_constraint_values;

		    IF v_constraint_values IS NOT NULL THEN
		        v_sql := concat('ALTER TABLE ', v_child_table, ' ADD CONSTRAINT ', v_constraint_name
		            , ' CHECK (', v_col, ' >= ', quote_literal(v_constraint_values.min), ' AND '
		            , v_col, ' <= ', quote_literal(v_constraint_values.max), ')' );
		        IF p_debug THEN
		            RAISE NOTICE 'Constraint creation query: %', v_sql;
		        END IF;
		        EXECUTE v_sql;

		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'New constraint created: '||v_sql);
		        END IF;
		    ELSE
		        IF p_debug THEN
		            RAISE NOTICE 'Given column (%) contains all NULLs. No constraint created', v_col;
		        END IF;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'NOTICE', 'Given column ('||v_col||') contains all NULLs. No constraint created');
		        END IF;
		    END IF;

		END LOOP;

		IF p_analyze THEN
		    EXECUTE 'ANALYZE '||p_parent_table;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE CONSTRAINT: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// apply_foreign_keys
	_, err = tx.Exec(`
		/*
		 * Apply foreign keys that exist on the given parent to the given child table
		 */
		CREATE FUNCTION partman.apply_foreign_keys(p_parent_table text, p_child_table text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
		    LANGUAGE plpgsql
		    AS $$
		DECLARE

		v_job_id            bigint;
		v_jobmon            text;
		v_jobmon_schema     text;
		v_old_search_path   text;
		v_ref_schema        text;
		v_ref_table         text;
		v_row               record;
		v_schemaname        text;
		v_sql               text;
		v_step_id           bigint;
		v_tablename         text;

		BEGIN

		SELECT jobmon INTO v_jobmon FROM partman.part_config WHERE parent_table = p_parent_table;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN APPLYING FOREIGN KEYS: '||p_parent_table);
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Checking if target child table exists');
		END IF;

		SELECT schemaname, tablename INTO v_schemaname, v_tablename 
		FROM pg_catalog.pg_tables 
		WHERE schemaname||'.'||tablename = p_child_table;

		IF v_tablename IS NULL THEN
		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'CRITICAL', 'Target child table ('||v_child_table||') does not exist.');
		        PERFORM fail_job(v_job_id);
		        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		    END IF;
		    RAISE EXCEPTION 'Target child table (%.%) does not exist.', v_schemaname, v_tablename;
		    RETURN;
		ELSE
		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;
		END IF;

		FOR v_row IN 
		    SELECT n.nspname||'.'||cl.relname AS ref_table
		        , '"'||string_agg(att.attname, '","')||'"' AS ref_column
		        , '"'||string_agg(att2.attname, '","')||'"' AS child_column
		    FROM
		        ( SELECT con.conname
		                , unnest(con.conkey) as ref
		                , unnest(con.confkey) as child
		                , con.confrelid
		                , con.conrelid
		          FROM pg_catalog.pg_class c
		          JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		          JOIN pg_catalog.pg_constraint con ON c.oid = con.conrelid
		          WHERE n.nspname ||'.'|| c.relname = p_parent_table
		          AND con.contype = 'f'
		          ORDER BY con.conkey
		    ) keys
		    JOIN pg_catalog.pg_class cl ON cl.oid = keys.confrelid
		    JOIN pg_catalog.pg_namespace n ON cl.relnamespace = n.oid
		    JOIN pg_catalog.pg_attribute att ON att.attrelid = keys.confrelid AND att.attnum = keys.child
		    JOIN pg_catalog.pg_attribute att2 ON att2.attrelid = keys.conrelid AND att2.attnum = keys.ref
		    GROUP BY keys.conname, n.nspname, cl.relname
		LOOP
		    SELECT schemaname, tablename INTO v_ref_schema, v_ref_table FROM pg_tables WHERE schemaname||'.'||tablename = v_row.ref_table;
		    v_sql := format('ALTER TABLE %I.%I ADD FOREIGN KEY (%s) REFERENCES %I.%I (%s)', 
		        v_schemaname, v_tablename, v_row.child_column, v_ref_schema, v_ref_table, v_row.ref_column);

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Applying FK: '||v_sql);
		    END IF;

		    EXECUTE v_sql;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'FK applied');
		    END IF;

		END LOOP;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN APPLYING FOREIGN KEYS: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// check_name_length()
	_, err = tx.Exec(`
		/*
		 * Truncate the name of the given object if it is greater than the postgres default max (63 characters).
		 * Also appends given suffix and schema if given and truncates the name so that the entire suffix will fit.
		 * Returns original name with schema given if it doesn't require truncation
		 */
		CREATE FUNCTION partman.check_name_length (p_object_name text, p_object_schema text DEFAULT NULL, p_suffix text DEFAULT NULL, p_table_partition boolean DEFAULT FALSE) RETURNS text
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE
		    v_new_length    int;
		    v_new_name      text;
		BEGIN

		IF p_table_partition IS TRUE AND (p_suffix IS NULL OR p_object_schema IS NULL) THEN
		    RAISE EXCEPTION 'Table partition name requires a schema and suffix value';
		END IF;

		IF p_table_partition THEN  -- 61 characters to account for _p in partition name
		    IF char_length(p_object_name) + char_length(p_suffix) >= 61 THEN
		        v_new_length := 61 - char_length(p_suffix);
		        v_new_name := p_object_schema ||'.'|| substring(p_object_name from 1 for v_new_length) || '_p' || p_suffix; 
		    ELSE
		        v_new_name := p_object_schema ||'.'||p_object_name||'_p'||p_suffix;
		    END IF;
		ELSE
		    IF char_length(p_object_name) + char_length(COALESCE(p_suffix, '')) >= 63 THEN
		        v_new_length := 63 - char_length(COALESCE(p_suffix, ''));
		        v_new_name := COALESCE(p_object_schema ||'.', '') || substring(p_object_name from 1 for v_new_length) || COALESCE(p_suffix, ''); 
		    ELSE
		        v_new_name := COALESCE(p_object_schema ||'.', '') || p_object_name||COALESCE(p_suffix, '');
		    END IF;
		END IF;

		RETURN v_new_name;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// check_parent()
	_, err = tx.Exec(`
		/*
		 * Function to monitor for data getting inserted into parent tables managed by extension
		 */
		CREATE FUNCTION partman.check_parent() RETURNS SETOF partman.check_parent_table
		    LANGUAGE plpgsql STABLE SECURITY DEFINER
		    AS $$
		DECLARE 
		    
		v_count 	bigint = 0;
		v_sql       text;
		v_tables 	record;
		v_trouble   partman.check_parent_table%rowtype;

		BEGIN

		FOR v_tables IN 
		    SELECT DISTINCT parent_table FROM partman.part_config
		LOOP

		    v_sql := 'SELECT count(1) AS n FROM ONLY '||v_tables.parent_table;
		    EXECUTE v_sql INTO v_count;

		    IF v_count > 0 THEN 
		        v_trouble.parent_table := v_tables.parent_table;
		        v_trouble.count := v_count;
		        RETURN NEXT v_trouble;
		    END IF;

			v_count := 0;

		END LOOP;

		RETURN;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// check_version()
	_, err = tx.Exec(`
		/*
		 * Check PostgreSQL version number. Parameter must be full 3 point version.
		 * Returns true if current version is greater than or equal to the parameter given.
		 */
		CREATE FUNCTION partman.check_version(p_check_version text) RETURNS boolean
		    LANGUAGE plpgsql STABLE
		    AS $$
		DECLARE

		v_check_version     text[];
		v_current_version   text[] := string_to_array(current_setting('server_version'), '.');
		 
		BEGIN

		v_check_version := string_to_array(p_check_version, '.');

		IF v_current_version[1]::int > v_check_version[1]::int THEN
		    RETURN true;
		END IF;
		IF v_current_version[1]::int = v_check_version[1]::int THEN
		    IF v_current_version[2]::int > v_check_version[2]::int THEN
		        RETURN true;
		    END IF;
		    IF v_current_version[2]::int = v_check_version[2]::int THEN
		        IF v_current_version[3]::int >= v_check_version[3]::int THEN
		            RETURN true;
		        END IF; -- 0.0.x
		    END IF; -- 0.x.0
		END IF; -- x.0.0

		RETURN false;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_function_id
	_, err = tx.Exec(`
		/*
		 * Create the trigger function for the parent table of an id-based partition set
		 */
		CREATE FUNCTION partman.create_function_id(p_parent_table text) RETURNS void
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_control                       text;
		v_count                         int;
		v_current_partition_name        text;
		v_current_partition_id          bigint;
		v_datetime_string               text;
		v_final_partition_id            bigint;
		v_function_name                 text;
		v_higher_parent                 text := p_parent_table;
		v_id_position                   int;
		v_job_id                        bigint;
		v_jobmon                        text;
		v_jobmon_schema                 text;
		v_last_partition                text;
		v_max                           bigint;
		v_next_partition_id             bigint;
		v_next_partition_name           text;
		v_old_search_path               text;
		v_parent_schema                 text;
		v_parent_tablename              text;
		v_part_interval                 bigint;
		v_premake                       int;
		v_prev_partition_id             bigint;
		v_prev_partition_name           text;
		v_run_maint                     boolean;
		v_step_id                       bigint;
		v_top_parent                    text := p_parent_table;
		v_trig_func                     text;
		v_type                          text;

		BEGIN

		SELECT type
		    , part_interval::bigint
		    , control
		    , premake
		    , use_run_maintenance
		    , jobmon
		INTO v_type
		    , v_part_interval
		    , v_control
		    , v_premake
		    , v_run_maint
		    , v_jobmon
		FROM partman.part_config 
		WHERE parent_table = p_parent_table
		AND (type = 'id-static' OR type = 'id-dynamic');

		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		SELECT show_partitions INTO v_last_partition FROM partman.show_partitions(p_parent_table, 'DESC') LIMIT 1;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN CREATE FUNCTION: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Creating partition function for table '||p_parent_table);
		END IF;

		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;
		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);

		IF v_type = 'id-static' THEN
		    -- Get the highest level top parent if multi-level partitioned in order to get proper max() value below
		    WHILE v_higher_parent IS NOT NULL LOOP -- initially set in DECLARE
		        WITH top_oid AS (
		            SELECT i.inhparent AS top_parent_oid
		            FROM pg_catalog.pg_inherits i
		            JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
		            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		            WHERE n.nspname||'.'||c.relname = v_higher_parent
		        ) SELECT n.nspname||'.'||c.relname
		        INTO v_higher_parent
		        FROM pg_catalog.pg_class c
		        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		        JOIN top_oid t ON c.oid = t.top_parent_oid
		        JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname
		        WHERE p.type = 'id-static' OR p.type = 'id-dynamic';

		        IF v_higher_parent IS NOT NULL THEN
		            -- initially set in DECLARE
		            v_top_parent := v_higher_parent;
		        END IF;

		    END LOOP;

		    EXECUTE 'SELECT COALESCE(max('||v_control||'), 0) FROM '||v_top_parent INTO v_max;
		    v_current_partition_id = v_max - (v_max % v_part_interval);
		    v_next_partition_id := v_current_partition_id + v_part_interval;
		    v_current_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_current_partition_id::text, TRUE);

		    v_trig_func := 'CREATE OR REPLACE FUNCTION '||v_function_name||'() RETURNS trigger LANGUAGE plpgsql AS $t$ 
		        DECLARE
		            v_current_partition_id  bigint;
		            v_last_partition        text := '||quote_literal(v_last_partition)||';
		            v_id_position           int;
		            v_next_partition_id     bigint;
		            v_next_partition_name   text;         
		            v_partition_created     boolean;
		        BEGIN
		        IF TG_OP = ''INSERT'' THEN 
		            IF NEW.'||v_control||' >= '||v_current_partition_id||' AND NEW.'||v_control||' < '||v_next_partition_id|| ' THEN ';
		            SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_current_partition_name;
		            IF v_count > 0 THEN
		                v_trig_func := v_trig_func || ' 
		                INSERT INTO '||v_current_partition_name||' VALUES (NEW.*); ';
		            ELSE
		                v_trig_func := v_trig_func || '
		                -- Child table for current values does not exist in this partition set, so write to parent
		                RETURN NEW;';
		            END IF;

		        FOR i IN 1..v_premake LOOP
		            v_prev_partition_id := v_current_partition_id - (v_part_interval * i);
		            v_next_partition_id := v_current_partition_id + (v_part_interval * i);
		            v_final_partition_id := v_next_partition_id + v_part_interval;
		            v_prev_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_prev_partition_id::text, TRUE);
		            v_next_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_next_partition_id::text, TRUE);
		            
		            -- Check that child table exist before making a rule to insert to them.
		            -- Handles edge case of changing premake immediately after running create_parent(). 
		            SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_prev_partition_name;
		            IF v_count > 0 THEN
		                -- Only handle previous partitions if they're starting above zero
		                IF v_prev_partition_id >= 0 THEN
		                    v_trig_func := v_trig_func ||'
		            ELSIF NEW.'||v_control||' >= '||v_prev_partition_id||' AND NEW.'||v_control||' < '||v_prev_partition_id + v_part_interval|| ' THEN 
		                INSERT INTO '||v_prev_partition_name||' VALUES (NEW.*); ';
		                END IF;
		            END IF;
		            
		            SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_next_partition_name;
		            IF v_count > 0 THEN
		                v_trig_func := v_trig_func ||'
		            ELSIF NEW.'||v_control||' >= '||v_next_partition_id||' AND NEW.'||v_control||' < '||v_final_partition_id|| ' THEN 
		                INSERT INTO '||v_next_partition_name||' VALUES (NEW.*);'; 
		            END IF;
		        END LOOP;
		        v_trig_func := v_trig_func ||'
		            ELSE
		                RETURN NEW;
		            END IF;';

		        IF v_run_maint IS FALSE THEN
		            v_trig_func := v_trig_func ||'
		            v_current_partition_id := NEW.'||v_control||' - (NEW.'||v_control||' % '||v_part_interval||');
		            IF (NEW.'||v_control||' % '||v_part_interval||') > ('||v_part_interval||' / 2) THEN
		                v_id_position := (length(v_last_partition) - position(''p_'' in reverse(v_last_partition))) + 2;
		                v_next_partition_id := (substring(v_last_partition from v_id_position)::bigint) + '||v_part_interval||';
		                WHILE ((v_next_partition_id - v_current_partition_id) / '||v_part_interval||') <= '||v_premake||' LOOP 
		                    v_partition_created := partman.create_partition_id('||quote_literal(p_parent_table)||', ARRAY[v_next_partition_id]);
		                    IF v_partition_created THEN
		                        PERFORM partman.create_function_id('||quote_literal(p_parent_table)||');
		                        PERFORM partman.apply_constraints('||quote_literal(p_parent_table)||');
		                    END IF;
		                    v_next_partition_id := v_next_partition_id + '||v_part_interval||';
		                END LOOP;
		            END IF;';
		        END IF;

		        v_trig_func := v_trig_func ||'
		        END IF; 
		        RETURN NULL; 
		        END $t$;';

		    EXECUTE v_trig_func;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Added function for current id interval: '||v_current_partition_id||' to '||v_final_partition_id-1);
		    END IF;

		ELSIF v_type = 'id-dynamic' THEN
		    -- The return inside the partition creation check is there to keep really high ID values from creating new partitions.
		    v_trig_func := 'CREATE OR REPLACE FUNCTION '||v_function_name||'() RETURNS trigger LANGUAGE plpgsql AS $t$ 
		        DECLARE
		            v_count                     int;
		            v_current_partition_id      bigint;
		            v_current_partition_name    text;
		            v_id_position               int;
		            v_last_partition            text := '||quote_literal(v_last_partition)||';
		            v_last_partition_id         bigint;
		            v_next_partition_id         bigint;
		            v_next_partition_name       text;   
		            v_partition_created         boolean;
		        BEGIN 
		        IF TG_OP = ''INSERT'' THEN 
		            v_current_partition_id := NEW.'||v_control||' - (NEW.'||v_control||' % '||v_part_interval||');
		            v_current_partition_name := partman.check_name_length('''||v_parent_tablename||''', '''||v_parent_schema||''', v_current_partition_id::text, TRUE);
		            SELECT count(*) INTO v_count FROM pg_tables WHERE schemaname ||''.''|| tablename = v_current_partition_name;
		            IF v_count > 0 THEN 
		                EXECUTE ''INSERT INTO ''||v_current_partition_name||'' VALUES($1.*)'' USING NEW;
		            ELSE
		                RETURN NEW;
		            END IF;';

		       IF v_run_maint IS FALSE THEN
		            v_trig_func := v_trig_func ||'
		            IF (NEW.'||v_control||' % '||v_part_interval||') > ('||v_part_interval||' / 2) THEN
		                v_id_position := (length(v_last_partition) - position(''p_'' in reverse(v_last_partition))) + 2;
		                v_last_partition_id = substring(v_last_partition from v_id_position)::bigint;
		                v_next_partition_id := v_last_partition_id + '||v_part_interval||';
		                IF NEW.'||v_control||' >= v_next_partition_id THEN
		                    RETURN NEW;
		                END IF;
		                WHILE ((v_next_partition_id - v_current_partition_id) / '||v_part_interval||') <= '||v_premake||' LOOP 
		                    v_partition_created := partman.create_partition_id('||quote_literal(p_parent_table)||', ARRAY[v_next_partition_id]);
		                    IF v_partition_created THEN
		                        PERFORM partman.create_function_id('||quote_literal(p_parent_table)||');
		                        PERFORM partman.apply_constraints('||quote_literal(p_parent_table)||');
		                    END IF;
		                    v_next_partition_id := v_next_partition_id + '||v_part_interval||';
		                END LOOP;
		            END IF;';
		        END IF;

		        v_trig_func := v_trig_func ||'
		        END IF;
		        RETURN NULL; 
		        END $t$;';

		    EXECUTE v_trig_func;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Added function for dynamic id table: '||p_parent_table);
		    END IF;

		ELSE
		    RAISE EXCEPTION 'ERROR: Invalid id partitioning type given: %', v_type;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE FUNCTION: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''Partition function maintenance for table '||p_parent_table||' failed'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_function_time()
	_, err = tx.Exec(`
		/*
		 * Create the trigger function for the parent table of a time-based partition set
		 */
		CREATE FUNCTION partman.create_function_time(p_parent_table text) RETURNS void
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_control                       text;
		v_count                         int;
		v_current_partition_name        text;
		v_current_partition_timestamp   timestamptz;
		v_datetime_string               text;
		v_final_partition_timestamp     timestamptz;
		v_function_name                 text;
		v_job_id                        bigint;
		v_jobmon                        boolean;
		v_jobmon_schema                 text;
		v_old_search_path               text;
		v_new_length                    int;
		v_next_partition_name           text;
		v_next_partition_timestamp      timestamptz;
		v_parent_schema                 text;
		v_parent_tablename              text;
		v_part_interval                 interval;
		v_premake                       int;
		v_prev_partition_name           text;
		v_prev_partition_timestamp      timestamptz;
		v_step_id                       bigint;
		v_trig_func                     text;
		v_type                          text;

		BEGIN

		SELECT type
		    , part_interval::interval
		    , control
		    , premake
		    , datetime_string
		    , jobmon
		INTO v_type
		    , v_part_interval
		    , v_control
		    , v_premake
		    , v_datetime_string
		    , v_jobmon
		FROM partman.part_config 
		WHERE parent_table = p_parent_table
		AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom');

		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN CREATE FUNCTION: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Creating partition function for table '||p_parent_table);
		END IF;

		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;

		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);

		IF v_type = 'time-static' THEN
		    CASE
		        WHEN v_part_interval = '15 mins' THEN
		            v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP) + 
		                '15min'::interval * floor(date_part('minute', CURRENT_TIMESTAMP) / 15.0);
		        WHEN v_part_interval = '30 mins' THEN
		            v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP) + 
		                '30min'::interval * floor(date_part('minute', CURRENT_TIMESTAMP) / 30.0);
		        WHEN v_part_interval = '1 hour' THEN
		            v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP);
		         WHEN v_part_interval = '1 day' THEN
		            v_current_partition_timestamp := date_trunc('day', CURRENT_TIMESTAMP);
		        WHEN v_part_interval = '1 week' THEN
		            v_current_partition_timestamp := date_trunc('week', CURRENT_TIMESTAMP);
		        WHEN v_part_interval = '1 month' THEN
		            v_current_partition_timestamp := date_trunc('month', CURRENT_TIMESTAMP);
		        -- Type time-static plus this interval is the special quarterly interval 
		        WHEN v_part_interval = '3 months' THEN
		            v_current_partition_timestamp := date_trunc('quarter', CURRENT_TIMESTAMP);
		        WHEN v_part_interval = '1 year' THEN
		            v_current_partition_timestamp := date_trunc('year', CURRENT_TIMESTAMP);
		    END CASE;

		    v_current_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, to_char(v_current_partition_timestamp, v_datetime_string), TRUE); 
		    v_next_partition_timestamp := v_current_partition_timestamp + v_part_interval::interval;

		    v_trig_func := 'CREATE OR REPLACE FUNCTION '||v_function_name||'() RETURNS trigger LANGUAGE plpgsql AS $t$ 
		        BEGIN 
		        IF TG_OP = ''INSERT'' THEN 
		            IF NEW.'||v_control||' >= '||quote_literal(v_current_partition_timestamp)||' AND NEW.'||v_control||' < '||quote_literal(v_next_partition_timestamp)|| ' THEN ';
		        SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_current_partition_name;
		        IF v_count > 0 THEN
		            v_trig_func := v_trig_func || '
		                INSERT INTO '||v_current_partition_name||' VALUES (NEW.*); ';
		        ELSE
		            v_trig_func := v_trig_func || '
		                -- Child table for current values does not exist in this partition set, so write to parent
		                RETURN NEW;';
		        END IF;
		    FOR i IN 1..v_premake LOOP
		        v_prev_partition_timestamp := v_current_partition_timestamp - (v_part_interval::interval * i);
		        v_next_partition_timestamp := v_current_partition_timestamp + (v_part_interval::interval * i);
		        v_final_partition_timestamp := v_next_partition_timestamp + (v_part_interval::interval);
		        v_prev_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, to_char(v_prev_partition_timestamp, v_datetime_string), TRUE);
		        v_next_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, to_char(v_next_partition_timestamp, v_datetime_string), TRUE);

		        -- Check that child table exist before making a rule to insert to them.
		        -- Handles edge case of changing premake immediately after running create_parent(). 
		        SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_prev_partition_name;
		        IF v_count > 0 THEN
		            v_trig_func := v_trig_func ||'
		            ELSIF NEW.'||v_control||' >= '||quote_literal(v_prev_partition_timestamp)||' AND NEW.'||v_control||' < '||
		                    quote_literal(v_prev_partition_timestamp + v_part_interval::interval)|| ' THEN 
		                INSERT INTO '||v_prev_partition_name||' VALUES (NEW.*);';
		        END IF;
		        SELECT count(*) INTO v_count FROM pg_catalog.pg_tables WHERE schemaname ||'.'||tablename = v_next_partition_name;
		        IF v_count > 0 THEN
		            v_trig_func := v_trig_func ||' 
		            ELSIF NEW.'||v_control||' >= '||quote_literal(v_next_partition_timestamp)||' AND NEW.'||v_control||' < '||
		                quote_literal(v_final_partition_timestamp)|| ' THEN 
		                INSERT INTO '||v_next_partition_name||' VALUES (NEW.*);';
		        END IF;

		    END LOOP;
		    v_trig_func := v_trig_func ||' 
		            ELSE 
		                RETURN NEW; 
		            END IF; 
		        END IF; 
		        RETURN NULL; 
		        END $t$;';

		    EXECUTE v_trig_func;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Added function for current time interval: '||
		            v_current_partition_timestamp||' to '||(v_final_partition_timestamp-'1sec'::interval));
		    END IF;

		ELSIF v_type = 'time-dynamic' THEN

		    v_trig_func := 'CREATE OR REPLACE FUNCTION '||v_function_name||'() RETURNS trigger LANGUAGE plpgsql AS $t$ 
		        DECLARE
		            v_count                 int;
		            v_partition_name        text;
		            v_partition_timestamp   timestamptz;
		        BEGIN 
		        IF TG_OP = ''INSERT'' THEN 
		            ';
		        CASE
		            WHEN v_part_interval = '15 mins' THEN 
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''hour'', NEW.'||v_control||') + 
		                    ''15min''::interval * floor(date_part(''minute'', NEW.'||v_control||') / 15.0);';
		            WHEN v_part_interval = '30 mins' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''hour'', NEW.'||v_control||') + 
		                    ''30min''::interval * floor(date_part(''minute'', NEW.'||v_control||') / 30.0);';
		            WHEN v_part_interval = '1 hour' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''hour'', NEW.'||v_control||');';
		             WHEN v_part_interval = '1 day' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''day'', NEW.'||v_control||');';
		            WHEN v_part_interval = '1 week' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''week'', NEW.'||v_control||');';
		            WHEN v_part_interval = '1 month' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''month'', NEW.'||v_control||');';
		            WHEN v_part_interval = '3 months' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''quarter'', NEW.'||v_control||');';
		            WHEN v_part_interval = '1 year' THEN
		                v_trig_func := v_trig_func||'v_partition_timestamp := date_trunc(''year'', NEW.'||v_control||');';
		        END CASE;
		        v_trig_func := v_trig_func||'
		            v_partition_name := partman.check_name_length('''||v_parent_tablename||''', '''||v_parent_schema||''', to_char(v_partition_timestamp, '||quote_literal(v_datetime_string)||'), TRUE);
		            SELECT count(*) INTO v_count FROM pg_tables WHERE schemaname ||''.''|| tablename = v_partition_name;
		            IF v_count > 0 THEN 
		                EXECUTE ''INSERT INTO ''||v_partition_name||'' VALUES($1.*)'' USING NEW;
		            ELSE
		                RETURN NEW;
		            END IF;
		        END IF;
		        
		        RETURN NULL; 
		        END $t$;';

		    EXECUTE v_trig_func;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Added function for dynamic time table: '||p_parent_table);
		    END IF;

		ELSIF v_type = 'time-custom' THEN

		    v_trig_func := 'CREATE OR REPLACE FUNCTION '||v_function_name||'() RETURNS trigger LANGUAGE plpgsql AS $t$ 
		        DECLARE
		            v_child_table       text;
		            v_count             int; 
		        BEGIN 

		        SELECT child_table INTO v_child_table
		        FROM partman.custom_time_partitions 
		        WHERE partition_range @> NEW.'||v_control||' 
		        AND parent_table = '||quote_literal(p_parent_table)||';

		        SELECT count(*) INTO v_count FROM pg_tables WHERE schemaname ||''.''|| tablename = v_child_table;
		        IF v_count > 0 THEN
		            EXECUTE ''INSERT INTO ''||v_child_table||'' VALUES ($1.*)'' USING NEW;
		        ELSE
		            RETURN NEW;
		        END IF;

		        RETURN NULL; 
		        END $t$;';

		    EXECUTE v_trig_func;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Added function for custom time table: '||p_parent_table);
		    END IF;

		ELSE
		    RAISE EXCEPTION 'ERROR: Invalid time partitioning type given: %', v_type;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE FUNCTION: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_parent()
	_, err = tx.Exec(`
	/*
	 * Function to turn a table into the parent of a partition set
	 */
	CREATE FUNCTION partman.create_parent(
	    p_parent_table text
	    , p_control text
	    , p_type text
	    , p_interval text
	    , p_constraint_cols text[] DEFAULT NULL 
	    , p_premake int DEFAULT 4
	    , p_use_run_maintenance boolean DEFAULT NULL
	    , p_start_partition text DEFAULT NULL
	    , p_inherit_fk boolean DEFAULT true
	    , p_jobmon boolean DEFAULT true
	    , p_debug boolean DEFAULT false) 
	RETURNS boolean 
	    LANGUAGE plpgsql SECURITY DEFINER
	    AS $$
	DECLARE

	v_base_timestamp                timestamp;
	v_count                         int := 1;
	v_datetime_string               text;
	v_higher_parent                 text := p_parent_table;
	v_id_interval                   bigint;
	v_id_position                   int;
	v_job_id                        bigint;
	v_jobmon_schema                 text;
	v_last_partition_created        boolean;
	v_max                           bigint;
	v_notnull                       boolean;
	v_old_search_path               text;
	v_parent_partition_id           bigint;
	v_parent_partition_timestamp    timestamp;
	v_partition_time                timestamp;
	v_partition_time_array          timestamp[];
	v_partition_id_array            bigint[];
	v_row                           record;
	v_run_maint                     boolean;
	v_sql                           text;
	v_start_time                    timestamp;
	v_starting_partition_id         bigint;
	v_step_id                       bigint;
	v_step_overflow_id              bigint;
	v_sub_parent                    text;
	v_success                       boolean := false;
	v_tablename                     text;
	v_time_interval                 interval;
	v_time_position                 int;
	v_top_parent                    text := p_parent_table;

	BEGIN

	IF position('.' in p_parent_table) = 0  THEN
	    RAISE EXCEPTION 'Parent table must be schema qualified';
	END IF;

	SELECT tablename INTO v_tablename FROM pg_tables WHERE schemaname || '.' || tablename = p_parent_table;
	    IF v_tablename IS NULL THEN
	        RAISE EXCEPTION 'Please create given parent table first: %', p_parent_table;
	    END IF;

	SELECT attnotnull INTO v_notnull FROM pg_attribute WHERE attrelid = p_parent_table::regclass AND attname = p_control;
	    IF v_notnull = false OR v_notnull IS NULL THEN
	        RAISE EXCEPTION 'Control column (%) for parent table (%) must be NOT NULL', p_control, p_parent_table;
	    END IF;

	IF NOT partman.check_partition_type(p_type) THEN
	    RAISE EXCEPTION '% is not a valid partitioning type', p_type;
	END IF;

	IF p_type = 'time-custom' AND partman.check_version('9.2.0') IS FALSE THEN
	    RAISE EXCEPTION 'The "time-custom" type requires a minimum PostgreSQL version of 9.2.0';
	END IF;

	EXECUTE 'LOCK TABLE '||p_parent_table||' IN ACCESS EXCLUSIVE MODE';

	IF p_jobmon THEN
	    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
	    IF v_jobmon_schema IS NOT NULL THEN
	        SELECT current_setting('search_path') INTO v_old_search_path;
	        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
	    END IF;
	END IF;

	IF p_use_run_maintenance IS NOT NULL THEN
	    IF p_use_run_maintenance IS FALSE AND (p_type = 'time-static' OR p_type = 'time-dynamic' OR p_type = 'time-custom') THEN
	        RAISE EXCEPTION 'p_run_maintenance cannot be set to false for time based partitioning';
	    END IF;
	    v_run_maint := p_use_run_maintenance;
	ELSIF p_type = 'time-static' OR p_type = 'time-dynamic' OR p_type = 'time-custom' THEN
	    v_run_maint := TRUE;
	ELSIF p_type = 'id-static' OR p_type ='id-dynamic' THEN
	    v_run_maint := FALSE;
	ELSE
	    RAISE EXCEPTION 'use_run_maintenance value cannot be set NULL';
	END IF;

	IF v_jobmon_schema IS NOT NULL THEN
	    v_job_id := add_job('PARTMAN SETUP PARENT: '||p_parent_table);
	    v_step_id := add_step(v_job_id, 'Creating initial partitions on new parent table: '||p_parent_table);
	END IF;

	-- If this parent table has siblings that are also partitioned (subpartitions), ensure it gets added to part_config_sub table so future maintenance will subpartition it
	-- Just doing in a loop to avoid having to assign a bunch of variables (should only run once, if at all; constraint should enforce only one value.)
	FOR v_row IN 
	    WITH parent_table AS (
	        SELECT h.inhparent as parent_oid
	        from pg_inherits h
	        where h.inhrelid::regclass = p_parent_table::regclass
	    ), sibling_children as (
	        select i.inhrelid::regclass::text as tablename 
	        from pg_inherits i
	        join parent_table p on i.inhparent = p.parent_oid
	    )
	    SELECT DISTINCT sub_type
	        , sub_control
	        , sub_part_interval
	        , sub_constraint_cols
	        , sub_premake
	        , sub_inherit_fk
	        , sub_retention
	        , sub_retention_schema
	        , sub_retention_keep_table
	        , sub_retention_keep_index
	        , sub_use_run_maintenance
	        , sub_jobmon
	    FROM partman.part_config_sub a
	    JOIN sibling_children b on a.sub_parent = b.tablename LIMIT 1
	LOOP
	    INSERT INTO partman.part_config_sub (
	        sub_parent
	        , sub_type
	        , sub_control
	        , sub_part_interval
	        , sub_constraint_cols
	        , sub_premake
	        , sub_inherit_fk
	        , sub_retention
	        , sub_retention_schema
	        , sub_retention_keep_table
	        , sub_retention_keep_index
	        , sub_use_run_maintenance
	        , sub_jobmon)
	    VALUES (
	        p_parent_table
	        , v_row.sub_type
	        , v_row.sub_control
	        , v_row.sub_part_interval
	        , v_row.sub_constraint_cols
	        , v_row.sub_premake
	        , v_row.sub_inherit_fk
	        , v_row.sub_retention
	        , v_row.sub_retention_schema
	        , v_row.sub_retention_keep_table
	        , v_row.sub_retention_keep_index
	        , v_row.sub_use_run_maintenance
	        , v_row.sub_jobmon);
	END LOOP;

	IF p_type = 'time-static' OR p_type = 'time-dynamic' OR p_type = 'time-custom' THEN

	    CASE
	        WHEN p_interval = 'yearly' THEN
	            v_time_interval := '1 year';
	        WHEN p_interval = 'quarterly' THEN
	            v_time_interval := '3 months';
	        WHEN p_interval = 'monthly' THEN
	            v_time_interval := '1 month';
	        WHEN p_interval  = 'weekly' THEN
	            v_time_interval := '1 week';
	        WHEN p_interval = 'daily' THEN
	            v_time_interval := '1 day';
	        WHEN p_interval = 'hourly' THEN
	            v_time_interval := '1 hour';
	        WHEN p_interval = 'half-hour' THEN
	            v_time_interval := '30 mins';
	        WHEN p_interval = 'quarter-hour' THEN
	            v_time_interval := '15 mins';
	        ELSE
	            IF p_type <> 'time-custom' THEN
	                RAISE EXCEPTION 'Must use a predefined time interval if not using type "time-custom". See documentation.';
	            END IF;
	            v_time_interval := p_interval::interval;
	            IF v_time_interval < '1 second'::interval THEN
	                RAISE EXCEPTION 'Partitioning interval must be 1 second or greater';
	            END IF;
	    END CASE;

	    -- First partition is either the min premake or p_start_partition
	    v_start_time := COALESCE(p_start_partition::timestamp, CURRENT_TIMESTAMP - (v_time_interval * p_premake));

	    IF v_time_interval >= '1 year' THEN
	        v_base_timestamp := date_trunc('year', v_start_time);
	        IF v_time_interval >= '10 years' THEN
	            v_base_timestamp := date_trunc('decade', v_start_time);
	            IF v_time_interval >= '100 years' THEN
	                v_base_timestamp := date_trunc('century', v_start_time);
	                IF v_time_interval >= '1000 years' THEN
	                    v_base_timestamp := date_trunc('millennium', v_start_time);
	                END IF; -- 1000
	            END IF; -- 100
	        END IF; -- 10
	    END IF; -- 1
	   
	    v_datetime_string := 'YYYY';
	    IF v_time_interval < '1 year' THEN
	        IF p_interval = 'quarterly' THEN
	            v_base_timestamp := date_trunc('quarter', v_start_time);
	            v_datetime_string = 'YYYY"q"Q';
	        ELSE
	            v_base_timestamp := date_trunc('month', v_start_time); 
	            v_datetime_string := v_datetime_string || '_MM';
	        END IF;
	        IF v_time_interval < '1 month' THEN
	            IF p_interval = 'weekly' THEN
	                v_base_timestamp := date_trunc('week', v_start_time);
	                v_datetime_string := 'IYYY"w"IW';
	            ELSE 
	                v_base_timestamp := date_trunc('day', v_start_time);
	                v_datetime_string := v_datetime_string || '_DD';
	            END IF;
	            IF v_time_interval < '1 day' THEN
	                v_base_timestamp := date_trunc('hour', v_start_time);
	                v_datetime_string := v_datetime_string || '_HH24MI';
	                IF v_time_interval < '1 minute' THEN
	                    v_base_timestamp := date_trunc('minute', v_start_time);
	                    v_datetime_string := v_datetime_string || 'SS';
	                END IF; -- minute
	            END IF; -- day
	        END IF; -- month
	    END IF; -- year

	    v_partition_time_array := array_append(v_partition_time_array, v_base_timestamp);
	    LOOP
	        -- If current loop value is less than or equal to the value of the max premake, add time to array.
	        IF (v_base_timestamp + (v_time_interval * v_count)) < (CURRENT_TIMESTAMP + (v_time_interval * p_premake)) THEN
	            BEGIN
	                v_partition_time := (v_base_timestamp + (v_time_interval * v_count))::timestamp;
	                v_partition_time_array := array_append(v_partition_time_array, v_partition_time);
	            EXCEPTION WHEN datetime_field_overflow THEN
	                RAISE WARNING 'Attempted partition time interval is outside PostgreSQL''s supported time range. 
	                    Child partition creation after time % skipped', v_partition_time;
	                v_step_overflow_id := add_step(v_job_id, 'Attempted partition time interval is outside PostgreSQL''s supported time range.');
	                PERFORM update_step(v_step_overflow_id, 'CRITICAL', 'Child partition creation after time '||v_partition_time||' skipped');
	                CONTINUE;
	            END;
	        ELSE
	            EXIT; -- all needed partitions added to array. Exit the loop.
	        END IF;
	        v_count := v_count + 1;
	    END LOOP;

	    INSERT INTO partman.part_config (
	        parent_table
	        , type
	        , part_interval
	        , control
	        , premake
	        , constraint_cols
	        , datetime_string
	        , use_run_maintenance
	        , inherit_fk
	        , jobmon) 
	    VALUES (
	        p_parent_table
	        , p_type
	        , v_time_interval
	        , p_control
	        , p_premake
	        , p_constraint_cols
	        , v_datetime_string
	        , v_run_maint
	        , p_inherit_fk
	        , p_jobmon);
	    v_last_partition_created := partman.create_partition_time(p_parent_table, v_partition_time_array, false);

	    IF v_last_partition_created = false THEN 
	        -- This can happen with subpartitioning when future or past partitions prevent child creation because they're out of range of the parent
	        -- First see if this parent is a subpartition managed by pg_partman
	        WITH top_oid AS (
	            SELECT i.inhparent AS top_parent_oid
	            FROM pg_catalog.pg_inherits i
	            JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
	            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	            WHERE n.nspname||'.'||c.relname = p_parent_table 
	        ) SELECT n.nspname||'.'||c.relname
	        INTO v_top_parent
	        FROM pg_catalog.pg_class c
	        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	        JOIN top_oid t ON c.oid = t.top_parent_oid
	        JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname;
	        IF v_top_parent IS NOT NULL THEN
	            -- If so create the lowest possible partition that is within the boundary of the parent
	            v_time_position := (length(p_parent_table) - position('p_' in reverse(p_parent_table))) + 2;
	            v_parent_partition_timestamp := to_timestamp(substring(p_parent_table from v_time_position), v_datetime_string);
	            IF v_base_timestamp >= v_parent_partition_timestamp THEN
	                WHILE v_base_timestamp >= v_parent_partition_timestamp LOOP
	                    v_base_timestamp := v_base_timestamp - v_time_interval;
	                END LOOP;
	                v_base_timestamp := v_base_timestamp + v_time_interval; -- add one back since while loop set it one lower than is needed
	            ELSIF v_base_timestamp < v_parent_partition_timestamp THEN
	                WHILE v_base_timestamp < v_parent_partition_timestamp LOOP
	                    v_base_timestamp := v_base_timestamp + v_time_interval;
	                END LOOP;
	                -- Don't need to remove one since new starting time will fit in top parent interval
	            END IF;
	            v_partition_time_array := NULL;
	            v_partition_time_array := array_append(v_partition_time_array, v_base_timestamp);
	            v_last_partition_created := partman.create_partition_time(p_parent_table, v_partition_time_array, false);
	        ELSE
	            -- Currently unknown edge case if code gets here
	            RAISE EXCEPTION 'No child tables created. Unexpected edge case encountered. Please report this error to author with conditions that led to it.';
	        END IF; 
	    END IF;

	    IF v_jobmon_schema IS NOT NULL THEN
	        PERFORM update_step(v_step_id, 'OK', 'Time partitions premade: '||p_premake);
	    END IF;
	END IF;

	IF p_type = 'id-static' OR p_type = 'id-dynamic' THEN
	    v_id_interval := p_interval::bigint;
	    IF v_id_interval <= 1 THEN
	        RAISE EXCEPTION 'Interval for serial partitioning must be greater than 1';
	    END IF;

	    -- Check if parent table is a subpartition of an already existing id partition set managed by pg_partman. 
	    WHILE v_higher_parent IS NOT NULL LOOP -- initially set in DECLARE
	        WITH top_oid AS (
	            SELECT i.inhparent AS top_parent_oid
	            FROM pg_catalog.pg_inherits i
	            JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
	            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	            WHERE n.nspname||'.'||c.relname = v_higher_parent
	        ) SELECT n.nspname||'.'||c.relname
	        INTO v_higher_parent
	        FROM pg_catalog.pg_class c
	        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	        JOIN top_oid t ON c.oid = t.top_parent_oid
	        JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname
	        WHERE p.type = 'id-static' OR p.type = 'id-dynamic';

	        IF v_higher_parent IS NOT NULL THEN
	            -- v_top_parent initially set in DECLARE
	            v_top_parent := v_higher_parent;
	        END IF;
	    END LOOP;

	    -- If custom start partition is set, use that.
	    -- If custom start is not set and there is already data, start partitioning with the highest current value and ensure it's grabbed from highest top parent table
	    v_sql := 'SELECT COALESCE('||quote_nullable(p_start_partition::bigint)||', max('||p_control||')::bigint, 0) FROM '||v_top_parent||' LIMIT 1';
	    EXECUTE v_sql INTO v_max;
	    v_starting_partition_id := v_max - (v_max % v_id_interval);
	    FOR i IN 0..p_premake LOOP
	        -- Only make previous partitions if ID value is less than the starting value and positive (and custom start partition wasn't set)
	        IF p_start_partition IS NULL AND 
	            (v_starting_partition_id - (v_id_interval*i)) > 0 AND 
	            (v_starting_partition_id - (v_id_interval*i)) < v_starting_partition_id 
	        THEN
	            v_partition_id_array = array_append(v_partition_id_array, (v_starting_partition_id - v_id_interval*i));
	        END IF; 
	        v_partition_id_array = array_append(v_partition_id_array, (v_id_interval*i) + v_starting_partition_id);
	    END LOOP;

	    INSERT INTO partman.part_config (
	        parent_table
	        , type
	        , part_interval
	        , control
	        , premake
	        , constraint_cols
	        , use_run_maintenance
	        , inherit_fk
	        , jobmon) 
	    VALUES (
	        p_parent_table
	        , p_type
	        , v_id_interval
	        , p_control
	        , p_premake
	        , p_constraint_cols
	        , v_run_maint
	        , p_inherit_fk
	        , p_jobmon);
	    v_last_partition_created := partman.create_partition_id(p_parent_table, v_partition_id_array, false);
	    IF v_last_partition_created = false THEN
	        -- This can happen with subpartitioning when future or past partitions prevent child creation because they're out of range of the parent
	        -- See if it's actually a subpartition of a parent id partition
	        WITH top_oid AS (
	            SELECT i.inhparent AS top_parent_oid
	            FROM pg_catalog.pg_inherits i
	            JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
	            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	            WHERE n.nspname||'.'||c.relname = p_parent_table 
	        ) SELECT n.nspname||'.'||c.relname
	        INTO v_top_parent
	        FROM pg_catalog.pg_class c
	        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	        JOIN top_oid t ON c.oid = t.top_parent_oid
	        JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname
	        WHERE p.type = 'id-static' OR p.type = 'id-dynamic';
	        IF v_top_parent IS NOT NULL THEN
	            -- Create the lowest possible partition that is within the boundary of the parent
	            v_id_position := (length(p_parent_table) - position('p_' in reverse(p_parent_table))) + 2;
	            v_parent_partition_id = substring(p_parent_table from v_id_position)::bigint;
	            IF v_starting_partition_id >= v_parent_partition_id THEN
	                WHILE v_starting_partition_id >= v_parent_partition_id LOOP
	                    v_starting_partition_id := v_starting_partition_id - v_id_interval;
	                END LOOP;
	                v_starting_partition_id := v_starting_partition_id + v_id_interval; -- add one back since while loop set it one lower than is needed
	            ELSIF v_starting_partition_id < v_parent_partition_id THEN
	                WHILE v_starting_partition_id < v_parent_partition_id LOOP
	                    v_starting_partition_id := v_starting_partition_id + v_id_interval;
	                END LOOP;
	                -- Don't need to remove one since new starting id will fit in top parent interval
	            END IF;
	            v_partition_id_array = NULL;
	            v_partition_id_array = array_append(v_partition_id_array, v_starting_partition_id);
	            v_last_partition_created := partman.create_partition_id(p_parent_table, v_partition_id_array, false);
	        ELSE
	            -- Currently unknown edge case if code gets here
	            RAISE EXCEPTION 'No child tables created. Unexpected edge case encountered. Please report this error to author with conditions that led to it.';
	        END IF;
	    END IF;
	END IF;

	IF v_jobmon_schema IS NOT NULL THEN
	    v_step_id := add_step(v_job_id, 'Creating partition function');
	END IF;
	IF p_type = 'time-static' OR p_type = 'time-dynamic' OR p_type = 'time-custom' THEN
	    PERFORM partman.create_function_time(p_parent_table);
	    IF v_jobmon_schema IS NOT NULL THEN
	        PERFORM update_step(v_step_id, 'OK', 'Time function created');
	    END IF;
	ELSIF p_type = 'id-static' OR p_type = 'id-dynamic' THEN
	    PERFORM partman.create_function_id(p_parent_table);  
	    IF v_jobmon_schema IS NOT NULL THEN
	        PERFORM update_step(v_step_id, 'OK', 'ID function created');
	    END IF;
	END IF;

	IF v_jobmon_schema IS NOT NULL THEN
	    v_step_id := add_step(v_job_id, 'Creating partition trigger');
	END IF;
	PERFORM partman.create_trigger(p_parent_table);

	IF v_jobmon_schema IS NOT NULL THEN
	    PERFORM update_step(v_step_id, 'OK', 'Done');
	    IF v_step_overflow_id IS NOT NULL THEN
	        PERFORM fail_job(v_job_id);
	    ELSE
	        PERFORM close_job(v_job_id);
	    END IF;
	    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
	END IF;

	v_success := true;

	RETURN v_success;

	EXCEPTION
	    WHEN OTHERS THEN
	        IF v_jobmon_schema IS NOT NULL THEN
	            IF v_job_id IS NULL THEN
	                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE PARENT: '||p_parent_table||''')' INTO v_job_id;
	                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''Partition creation for table '||p_parent_table||' failed'')' INTO v_step_id;
	            ELSIF v_step_id IS NULL THEN
	                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
	            END IF;
	            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
	            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
	        END IF;
	        RAISE EXCEPTION '%', SQLERRM;
	END
	$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_partition_id()
	_, err = tx.Exec(`
		/*
		 * Function to create id partitions
		 */
		CREATE FUNCTION partman.create_partition_id(p_parent_table text, p_partition_ids bigint[], p_analyze boolean DEFAULT true) RETURNS boolean
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_all               text[] := ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'];
		v_analyze           boolean := FALSE;
		v_control           text;
		v_grantees          text[];
		v_hasoids           boolean;
		v_id                bigint;
		v_id_position       int;
		v_inherit_fk        boolean;
		v_job_id            bigint;
		v_jobmon            boolean;
		v_jobmon_schema     text;
		v_old_search_path   text;
		v_parent_grant      record;
		v_parent_owner      text;
		v_parent_schema     text;
		v_parent_tablename  text;
		v_parent_tablespace text;
		v_part_interval     bigint;
		v_partition_created boolean := false;
		v_partition_name    text;
		v_revoke            text[];
		v_row               record;
		v_sql               text;
		v_step_id           bigint;
		v_sub_id_max        bigint;
		v_sub_id_min        bigint;
		v_tablename         text;
		v_top_interval      bigint;
		v_top_parent        text;
		v_unlogged          char;

		BEGIN

		SELECT control
		    , part_interval
		    , inherit_fk
		    , jobmon
		INTO v_control
		    , v_part_interval
		    , v_inherit_fk
		    , v_jobmon
		FROM partman.part_config
		WHERE parent_table = p_parent_table
		AND (type = 'id-static' OR type = 'id-dynamic');

		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		-- Check if parent table is a subpartition of an already existing id based partition set managed by pg_partman
		-- If so, limit what child tables can be created based on parent suffix
		WITH top_oid AS (
		    SELECT i.inhparent AS top_parent_oid
		    FROM pg_catalog.pg_class c
		    JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
		    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		    WHERE n.nspname||'.'||c.relname = p_parent_table
		) SELECT n.nspname||'.'||c.relname 
		  INTO v_top_parent 
		  FROM pg_catalog.pg_class c
		  JOIN top_oid t ON c.oid = t.top_parent_oid
		  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		  JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname
		  WHERE c.oid = t.top_parent_oid
		  AND p.type = 'id-static' OR p.type = 'id-dynamic';

		IF v_top_parent IS NOT NULL THEN 
		    SELECT part_interval::bigint INTO v_top_interval FROM partman.part_config WHERE parent_table = v_top_parent;
		    v_id_position := (length(p_parent_table) - position('p_' in reverse(p_parent_table))) + 2;
		    v_sub_id_min = substring(p_parent_table from v_id_position)::bigint;
		    v_sub_id_max = (v_sub_id_min + v_top_interval) - 1;
		END IF;

		SELECT tableowner, schemaname, tablename, tablespace INTO v_parent_owner, v_parent_schema, v_parent_tablename, v_parent_tablespace FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;

		FOREACH v_id IN ARRAY p_partition_ids LOOP
		-- Do not create the child table if it's outside the bounds of the top parent. 
		    IF v_sub_id_min IS NOT NULL THEN
		        IF v_id < v_sub_id_min OR v_id > v_sub_id_max THEN
		            CONTINUE;
		        END IF;
		    END IF;

		    v_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_id::text, TRUE);
		    -- If child table already exists, skip creation
		    SELECT tablename INTO v_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_partition_name;
		    IF v_tablename IS NOT NULL THEN
		        CONTINUE;
		    END IF;

		    -- Ensure analyze is run if a new partition is created. Otherwise if one isn't, will be false and analyze will be skipped
		    v_analyze := TRUE;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_job_id := add_job('PARTMAN CREATE TABLE: '||p_parent_table);
		        v_step_id := add_step(v_job_id, 'Creating new partition '||v_partition_name||' with interval from '||v_id||' to '||(v_id + v_part_interval)-1);
		    END IF;

		    SELECT relpersistence INTO v_unlogged FROM pg_catalog.pg_class WHERE oid::regclass = p_parent_table::regclass;
		    v_sql := 'CREATE';
		    IF v_unlogged = 'u' THEN
		        v_sql := v_sql || ' UNLOGGED';
		    END IF;
		    v_sql := v_sql || ' TABLE '||v_partition_name||' (LIKE '||p_parent_table||' INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING STORAGE INCLUDING COMMENTS)';
		    SELECT relhasoids INTO v_hasoids FROM pg_catalog.pg_class WHERE oid::regclass = p_parent_table::regclass;
		    IF v_hasoids IS TRUE THEN
		        v_sql := v_sql || ' WITH (OIDS)';
		    END IF;
		    EXECUTE v_sql;
		    SELECT tablename INTO v_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_partition_name;
		    IF v_parent_tablespace IS NOT NULL THEN
		        EXECUTE 'ALTER TABLE '||v_partition_name||' SET TABLESPACE '||v_parent_tablespace;
		    END IF;
		    EXECUTE 'ALTER TABLE '||v_partition_name||' ADD CONSTRAINT '||v_tablename||'_partition_check 
		        CHECK ('||v_control||'>='||quote_literal(v_id)||' AND '||v_control||'<'||quote_literal(v_id + v_part_interval)||')';
		    EXECUTE 'ALTER TABLE '||v_partition_name||' INHERIT '||p_parent_table;

		    FOR v_parent_grant IN 
		        SELECT array_agg(DISTINCT privilege_type::text ORDER BY privilege_type::text) AS types, grantee
		        FROM information_schema.table_privileges 
		        WHERE table_schema ||'.'|| table_name = p_parent_table
		        GROUP BY grantee 
		    LOOP
		        EXECUTE 'GRANT '||array_to_string(v_parent_grant.types, ',')||' ON '||v_partition_name||' TO '||v_parent_grant.grantee;
		        SELECT array_agg(r) INTO v_revoke FROM (SELECT unnest(v_all) AS r EXCEPT SELECT unnest(v_parent_grant.types)) x;
		        IF v_revoke IS NOT NULL THEN
		            EXECUTE 'REVOKE '||array_to_string(v_revoke, ',')||' ON '||v_partition_name||' FROM '||v_parent_grant.grantee||' CASCADE';
		        END IF;
		        v_grantees := array_append(v_grantees, v_parent_grant.grantee::text);
		    END LOOP;
		    -- Revoke all privileges from roles that have none on the parent
		    IF v_grantees IS NOT NULL THEN
		        SELECT array_agg(r) INTO v_revoke FROM (
		            SELECT DISTINCT grantee::text AS r FROM information_schema.table_privileges WHERE table_schema ||'.'|| table_name = v_partition_name
		            EXCEPT
		            SELECT unnest(v_grantees)) x;
		        IF v_revoke IS NOT NULL THEN
		            EXECUTE 'REVOKE ALL ON '||v_partition_name||' FROM '||array_to_string(v_revoke, ',');
		        END IF;
		    END IF;

		    EXECUTE 'ALTER TABLE '||v_partition_name||' OWNER TO '||v_parent_owner;

		    IF v_inherit_fk THEN
		        PERFORM partman.apply_foreign_keys(quote_ident(v_parent_schema)||'.'||quote_ident(v_parent_tablename), v_partition_name);
		    END IF;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;

		    -- Will only loop once and only if sub_partitioning is actually configured
		    -- This seemed easier than assigning a bunch of variables then doing an IF condition
		    FOR v_row IN 
		        SELECT sub_parent
		            , sub_control
		            , sub_type
		            , sub_part_interval
		            , sub_constraint_cols
		            , sub_premake
		            , sub_inherit_fk
		            , sub_retention
		            , sub_retention_schema
		            , sub_retention_keep_table
		            , sub_retention_keep_index
		            , sub_use_run_maintenance
		            , sub_jobmon
		        FROM partman.part_config_sub
		        WHERE sub_parent = p_parent_table
		    LOOP
		        IF v_jobmon_schema IS NOT NULL THEN
		            v_step_id := add_step(v_job_id, 'Subpartitioning '||v_partition_name);
		        END IF;
		        v_sql := format('SELECT partman.create_parent(
		                 p_parent_table := %L
		                , p_control := %L
		                , p_type := %L
		                , p_interval := %L
		                , p_constraint_cols := %L
		                , p_premake := %L
		                , p_use_run_maintenance := %L
		                , p_inherit_fk := %L
		                , p_jobmon := %L )'
		            , v_partition_name
		            , v_row.sub_control
		            , v_row.sub_type
		            , v_row.sub_part_interval
		            , v_row.sub_constraint_cols
		            , v_row.sub_premake
		            , v_row.sub_inherit_fk
		            , v_row.sub_use_run_maintenance
		            , v_row.sub_jobmon);
		        EXECUTE v_sql;

		        UPDATE partman.part_config SET 
		            retention_schema = v_row.sub_retention_schema
		            , retention_keep_table = v_row.sub_retention_keep_table
		            , retention_keep_index = v_row.sub_retention_keep_index
		        WHERE parent_table = v_partition_name;

		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Done');
		        END IF;

		    END LOOP; -- end sub partitioning LOOP

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM close_job(v_job_id);
		    END IF;
		    
		    v_partition_created := true;

		END LOOP;

		-- v_analyze is a local check if a new table is made.
		-- p_analyze is a parameter to say whether to run the analyze at all. Used by create_parent() to avoid long exclusive lock or run_maintenence() to avoid long creation runs.
		IF v_analyze AND p_analyze THEN
		    EXECUTE 'ANALYZE '||p_parent_table;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_partition_created;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE TABLE: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_partition_time()
	_, err = tx.Exec(`
		/*
		 * Function to create a child table in a time-based partition set
		 */
		CREATE FUNCTION partman.create_partition_time (p_parent_table text, p_partition_times timestamp[], p_analyze boolean DEFAULT true) 
		RETURNS boolean
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_all                           text[] := ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'];
		v_analyze                       boolean := FALSE;
		v_control                       text;
		v_datetime_string               text;
		v_grantees                      text[];
		v_hasoids                       boolean;
		v_inherit_fk                    boolean;
		v_job_id                        bigint;
		v_jobmon                        boolean;
		v_jobmon_schema                 text;
		v_old_search_path               text;
		v_parent_grant                  record;
		v_parent_owner                  text;
		v_parent_schema                 text;
		v_parent_tablename              text;
		v_partition_created             boolean := false;
		v_partition_name                text;
		v_partition_suffix              text;
		v_parent_tablespace             text;
		v_part_interval                 interval;
		v_partition_timestamp_end       timestamp;
		v_partition_timestamp_start     timestamp;
		v_quarter                       text;
		v_revoke                        text[];
		v_row                           record;
		v_sql                           text;
		v_step_id                       bigint;
		v_step_overflow_id              bigint;
		v_sub_timestamp_max             timestamp;
		v_sub_timestamp_min             timestamp;
		v_tablename                     text;
		v_time_position                 int;
		v_top_interval                  interval;
		v_top_parent                    text;
		v_trunc_value                   text;
		v_time                          timestamp;
		v_type                          text;
		v_unlogged                      char;
		v_year                          text;

		BEGIN

		SELECT type
		    , control
		    , part_interval
		    , inherit_fk
		    , jobmon
		    , datetime_string
		INTO v_type
		    , v_control
		    , v_part_interval
		    , v_inherit_fk
		    , v_jobmon
		    , v_datetime_string
		FROM partman.part_config
		WHERE parent_table = p_parent_table
		AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom');

		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		-- Check if parent table is a subpartition of an already existing time-based partition set managed by pg_partman
		-- If so, limit what child tables can be created based on parent suffix
		WITH top_oid AS (
		    SELECT i.inhparent AS top_parent_oid
		    FROM pg_catalog.pg_class c
		    JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
		    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		    WHERE n.nspname||'.'||c.relname = p_parent_table
		) SELECT n.nspname||'.'||c.relname 
		  INTO v_top_parent 
		  FROM pg_catalog.pg_class c
		  JOIN top_oid t ON c.oid = t.top_parent_oid
		  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		  JOIN partman.part_config p ON p.parent_table = n.nspname||'.'||c.relname
		  WHERE c.oid = t.top_parent_oid
		  AND p.type = 'time-static' OR p.type = 'time-dynamic';

		IF v_top_parent IS NOT NULL THEN 

		    SELECT part_interval::interval INTO v_top_interval FROM partman.part_config WHERE parent_table = v_top_parent;

		    v_time_position := (length(p_parent_table) - position('p_' in reverse(p_parent_table))) + 2;
		    IF v_part_interval::interval <> '3 months' OR (v_part_interval::interval = '3 months' AND v_type = 'time-custom') THEN
		       v_sub_timestamp_min := to_timestamp(substring(p_parent_table from v_time_position), v_datetime_string);
		    ELSE
		        -- to_timestamp doesn't recognize 'Q' date string formater. Handle it
		        v_year := split_part(substring(p_parent_table from v_time_position), 'q', 1);
		        v_quarter := split_part(substring(p_parent_table from v_time_position), 'q', 2);
		        CASE
		            WHEN v_quarter = '1' THEN
		                v_sub_timestamp_min := to_timestamp(v_year || '-01-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '2' THEN
		                v_sub_timestamp_min := to_timestamp(v_year || '-04-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '3' THEN
		                v_sub_timestamp_min := to_timestamp(v_year || '-07-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '4' THEN
		                v_sub_timestamp_min := to_timestamp(v_year || '-10-01', 'YYYY-MM-DD');
		        END CASE;
		    END IF;
		    v_sub_timestamp_max = (v_sub_timestamp_min + v_top_interval::interval) - '1 sec'::interval;

		END IF;

		SELECT tableowner, schemaname, tablename, tablespace INTO v_parent_owner, v_parent_schema, v_parent_tablename, v_parent_tablespace FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;

		FOREACH v_time IN ARRAY p_partition_times LOOP    
		    v_partition_timestamp_start := v_time;
		    BEGIN
		        v_partition_timestamp_end := v_time + v_part_interval;
		    EXCEPTION WHEN datetime_field_overflow THEN
		        RAISE WARNING 'Attempted partition time interval is outside PostgreSQL''s supported time range. 
		            Child partition creation after time % skipped', v_time;
		        v_step_overflow_id := add_step(v_job_id, 'Attempted partition time interval is outside PostgreSQL''s supported time range.');
		        PERFORM update_step(v_step_overflow_id, 'CRITICAL', 'Child partition creation after time '||v_time||' skipped');
		        CONTINUE;
		    END;

		    -- This suffix generation code is in partition_data_time() as well
		    v_partition_suffix := to_char(v_time, 'YYYY');
		    IF v_part_interval < '1 year' AND v_part_interval <> '1 week' THEN 
		        v_partition_suffix := v_partition_suffix ||'_'|| to_char(v_time, 'MM');
		        IF v_part_interval < '1 month' AND v_part_interval <> '1 week' THEN 
		            v_partition_suffix := v_partition_suffix ||'_'|| to_char(v_time, 'DD');
		            IF v_part_interval < '1 day' THEN
		                v_partition_suffix := v_partition_suffix || '_' || to_char(v_time, 'HH24MI');
		                IF v_part_interval < '1 minute' THEN
		                    v_partition_suffix := v_partition_suffix || to_char(v_time, 'SS');
		                END IF; -- end < minute IF
		            END IF; -- end < day IF      
		        END IF; -- end < month IF
		    END IF; -- end < year IF

		    IF v_part_interval = '1 week' THEN
		        v_partition_suffix := to_char(v_time, 'IYYY') || 'w' || to_char(v_time, 'IW');
		    END IF;

		    -- "Q" is ignored in to_timestamp, so handle special case
		    IF v_part_interval = '3 months' AND (v_type = 'time-static' OR v_type = 'time-dynamic') THEN
		        v_year := to_char(v_time, 'YYYY');
		        v_quarter := to_char(v_time, 'Q');
		        v_partition_suffix := v_year || 'q' || v_quarter;
		    END IF;


		-- Do not create the child table if it's outside the bounds of the top parent. 
		    IF v_sub_timestamp_min IS NOT NULL THEN
		        IF v_time < v_sub_timestamp_min OR v_time > v_sub_timestamp_max THEN
		            CONTINUE;
		        END IF;
		    END IF;

		    v_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_partition_suffix, TRUE);
		    SELECT tablename INTO v_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_partition_name;
		    IF v_tablename IS NOT NULL THEN
		        CONTINUE;
		    END IF;

		    -- Ensure analyze is run if a new partition is created. Otherwise if one isn't, will be false and analyze will be skipped
		    v_analyze := TRUE;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_job_id := add_job('PARTMAN CREATE TABLE: '||p_parent_table);
		        v_step_id := add_step(v_job_id, 'Creating new partition '||v_partition_name||' with interval from '||v_partition_timestamp_start||' to '||(v_partition_timestamp_end-'1sec'::interval));
		    END IF;

		    SELECT relpersistence INTO v_unlogged FROM pg_catalog.pg_class WHERE oid::regclass = p_parent_table::regclass;
		    v_sql := 'CREATE';
		    IF v_unlogged = 'u' THEN
		        v_sql := v_sql || ' UNLOGGED';
		    END IF;
		    v_sql := v_sql || ' TABLE '||v_partition_name||' (LIKE '||p_parent_table||' INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING STORAGE INCLUDING COMMENTS)';
		    SELECT relhasoids INTO v_hasoids FROM pg_catalog.pg_class WHERE oid::regclass = p_parent_table::regclass;
		    IF v_hasoids IS TRUE THEN
		        v_sql := v_sql || ' WITH (OIDS)';
		    END IF;
		    EXECUTE v_sql;
		    SELECT tablename INTO v_tablename FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_partition_name;
		    IF v_parent_tablespace IS NOT NULL THEN
		        EXECUTE 'ALTER TABLE '||v_partition_name||' SET TABLESPACE '||v_parent_tablespace;
		    END IF;
		    EXECUTE 'ALTER TABLE '||v_partition_name||' ADD CONSTRAINT '||v_tablename||'_partition_check
		        CHECK ('||v_control||'>='||quote_literal(v_partition_timestamp_start)||' AND '||v_control||'<'||quote_literal(v_partition_timestamp_end)||')';
		    EXECUTE 'ALTER TABLE '||v_partition_name||' INHERIT '||p_parent_table;

		    -- If custom time, set extra config options.
		    IF v_type = 'time-custom' THEN
		        INSERT INTO partman.custom_time_partitions (parent_table, child_table, partition_range)
		        VALUES ( p_parent_table, v_partition_name, tstzrange(v_partition_timestamp_start, v_partition_timestamp_end, '[)') );
		    END IF;

		    FOR v_parent_grant IN 
		        SELECT array_agg(DISTINCT privilege_type::text ORDER BY privilege_type::text) AS types, grantee
		        FROM information_schema.table_privileges 
		        WHERE table_schema ||'.'|| table_name = p_parent_table
		        GROUP BY grantee 
		    LOOP
		        EXECUTE 'GRANT '||array_to_string(v_parent_grant.types, ',')||' ON '||v_partition_name||' TO '||v_parent_grant.grantee;
		        SELECT array_agg(r) INTO v_revoke FROM (SELECT unnest(v_all) AS r EXCEPT SELECT unnest(v_parent_grant.types)) x;
		        IF v_revoke IS NOT NULL THEN
		            EXECUTE 'REVOKE '||array_to_string(v_revoke, ',')||' ON '||v_partition_name||' FROM '||v_parent_grant.grantee||' CASCADE';
		        END IF;
		        v_grantees := array_append(v_grantees, v_parent_grant.grantee::text);
		    END LOOP;
		    -- Revoke all privileges from roles that have none on the parent
		    IF v_grantees IS NOT NULL THEN
		        SELECT array_agg(r) INTO v_revoke FROM (
		            SELECT DISTINCT grantee::text AS r FROM information_schema.table_privileges WHERE table_schema ||'.'|| table_name = v_partition_name
		            EXCEPT
		            SELECT unnest(v_grantees)) x;
		        IF v_revoke IS NOT NULL THEN
		            EXECUTE 'REVOKE ALL ON '||v_partition_name||' FROM '||array_to_string(v_revoke, ',');
		        END IF;
		    END IF;

		    EXECUTE 'ALTER TABLE '||v_partition_name||' OWNER TO '||v_parent_owner;

		    IF v_inherit_fk THEN
		        PERFORM partman.apply_foreign_keys(quote_ident(v_parent_schema)||'.'||quote_ident(v_parent_tablename), v_partition_name);
		    END IF;

		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;

		    -- Will only loop once and only if sub_partitioning is actually configured
		    -- This seemed easier than assigning a bunch of variables then doing an IF condition
		    FOR v_row IN 
		        SELECT sub_parent
		            , sub_control
		            , sub_type
		            , sub_part_interval
		            , sub_constraint_cols
		            , sub_premake
		            , sub_inherit_fk
		            , sub_retention
		            , sub_retention_schema
		            , sub_retention_keep_table
		            , sub_retention_keep_index
		            , sub_use_run_maintenance
		            , sub_jobmon
		        FROM partman.part_config_sub
		        WHERE sub_parent = p_parent_table
		    LOOP
		        IF v_jobmon_schema IS NOT NULL THEN
		            v_step_id := add_step(v_job_id, 'Subpartitioning '||v_partition_name);
		        END IF;
		        v_sql := format('SELECT partman.create_parent(
		                 p_parent_table := %L
		                , p_control := %L
		                , p_type := %L
		                , p_interval := %L
		                , p_constraint_cols := %L
		                , p_premake := %L
		                , p_use_run_maintenance := %L
		                , p_inherit_fk := %L
		                , p_jobmon := %L )'
		            , v_partition_name
		            , v_row.sub_control
		            , v_row.sub_type
		            , v_row.sub_part_interval
		            , v_row.sub_constraint_cols
		            , v_row.sub_premake
		            , v_row.sub_inherit_fk
		            , v_row.sub_use_run_maintenance
		            , v_row.sub_jobmon);
		        EXECUTE v_sql;

		        UPDATE partman.part_config SET 
		            retention_schema = v_row.sub_retention_schema
		            , retention_keep_table = v_row.sub_retention_keep_table
		            , retention_keep_index = v_row.sub_retention_keep_index
		        WHERE parent_table = v_partition_name;

		    END LOOP; -- end sub partitioning LOOP

		    IF v_jobmon_schema IS NOT NULL THEN
		        IF v_step_overflow_id IS NOT NULL THEN
		            PERFORM fail_job(v_job_id);
		        ELSE
		            PERFORM close_job(v_job_id);
		        END IF;
		    END IF;

		    v_partition_created := true;

		END LOOP;

		-- v_analyze is a local check if a new table is made.
		-- p_analyze is a parameter to say whether to run the analyze at all. Used by create_parent() to avoid long exclusive lock or run_maintenence() to avoid long creation runs.
		IF v_analyze AND p_analyze THEN
		    EXECUTE 'ANALYZE '||p_parent_table;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_partition_created;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN CREATE TABLE: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_sub_parent()
	_, err = tx.Exec(`
		/*
		 * Create a partition set that is a subpartition of an already existing partition set.
		 * Given the parent table of any current partition set, it will turn all existing children into parent tables of their own partition sets
		 *      using the configuration options given as parameters to this function.
		 * Uses another config table that allows for turning all future child partitions into a new parent automatically.
		 * To avoid logical complications and contention issues, ALL subpartitions must be maintained using run_maintenance().
		 * This means the automatic, trigger based partition creation for serial partitioning will not work if it is a subpartition.
		 */
		CREATE FUNCTION partman.create_sub_parent(
		    p_top_parent text
		    , p_control text
		    , p_type text
		    , p_interval text
		    , p_constraint_cols text[] DEFAULT NULL 
		    , p_premake int DEFAULT 4
		    , p_start_partition text DEFAULT NULL
		    , p_inherit_fk boolean DEFAULT true
		    , p_jobmon boolean DEFAULT true
		    , p_debug boolean DEFAULT false) 
		RETURNS boolean
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_last_partition    text;
		v_row               record;
		v_row_last_part     record;
		v_run_maint         boolean;
		v_sql               text;
		v_success           boolean := false;
		v_top_type          text;

		BEGIN

		SELECT use_run_maintenance INTO v_run_maint FROM partman.part_config WHERE parent_table = p_top_parent;
		IF v_run_maint IS NULL THEN
		    RAISE EXCEPTION 'Cannot subpartition a table that is not managed by pg_partman already. Given top parent table not found in partman.part_config: %', p_top_parent;
		ELSIF v_run_maint = false THEN
		    RAISE EXCEPTION 'Any parent table that will be part of a sub-partitioned set (on any level) must have use_run_maintenance set to true in part_config table, even for serial partitioning. See documentation for more info.';
		END IF;

		FOR v_row IN 
		    -- Loop through all current children to turn them into partitioned tables
		    SELECT show_partitions AS child_table FROM partman.show_partitions(p_top_parent)
		LOOP
		    -- Just call existing create_parent() function but add the given parameters to the part_config_sub table as well
		    v_sql := format('SELECT partman.create_parent(
		             p_parent_table := %L
		            , p_control := %L
		            , p_type := %L
		            , p_interval := %L
		            , p_constraint_cols := %L
		            , p_premake := %L
		            , p_use_run_maintenance := %L
		            , p_start_partition := %L
		            , p_inherit_fk := %L
		            , p_jobmon := %L
		            , p_debug := %L )'
		        , v_row.child_table
		        , p_control
		        , p_type
		        , p_interval
		        , p_constraint_cols
		        , p_premake
		        , true
		        , p_start_partition
		        , p_inherit_fk
		        , p_jobmon
		        , p_debug);
		    EXECUTE v_sql;

		END LOOP;

		INSERT INTO partman.part_config_sub (
		    sub_parent
		    , sub_control
		    , sub_type
		    , sub_part_interval
		    , sub_constraint_cols
		    , sub_premake
		    , sub_inherit_fk
		    , sub_use_run_maintenance
		    , sub_jobmon)
		VALUES (
		    p_top_parent
		    , p_control
		    , p_type
		    , p_interval
		    , p_constraint_cols
		    , p_premake
		    , p_inherit_fk
		    , true
		    , p_jobmon);

		v_success := true;

		RETURN v_success;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// create_trigger()
	_, err = tx.Exec(`
		CREATE FUNCTION partman.create_trigger(p_parent_table text) RETURNS void
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_function_name         text;
		v_new_length            int;
		v_parent_schema         text;
		v_parent_tablename      text;
		v_trig_name             text;
		v_trig_sql              text;

		BEGIN

		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;
		v_trig_name := partman.check_name_length(p_object_name := v_parent_tablename, p_suffix := '_part_trig'); 
		-- Ensure function name matches the naming pattern
		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);
		v_trig_sql := 'CREATE TRIGGER '||v_trig_name||' BEFORE INSERT ON '||p_parent_table||
		    ' FOR EACH ROW EXECUTE PROCEDURE '||v_function_name||'()';

		EXECUTE v_trig_sql;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// drop_constraints()
	_, err = tx.Exec(`
		/*
		 * Drop constraints managed by pg_partman
		 */
		CREATE FUNCTION partman.drop_constraints(p_parent_table text, p_child_table text, p_debug boolean DEFAULT false) RETURNS void
		    LANGUAGE plpgsql
		    AS $$
		DECLARE

		v_col                           text;
		v_constraint_cols               text[]; 
		v_existing_constraint_name      text;
		v_exists                        boolean := FALSE;
		v_job_id                        bigint;
		v_jobmon                        boolean;
		v_jobmon_schema                 text;
		v_old_search_path               text;
		v_sql                           text;
		v_step_id                       bigint;

		BEGIN

		SELECT constraint_cols 
		    , jobmon
		INTO v_constraint_cols 
		    , v_jobmon
		FROM partman.part_config 
		WHERE parent_table = p_parent_table;

		IF v_constraint_cols IS NULL THEN
		    RAISE EXCEPTION 'Given parent table (%) not set up for constraint management (constraint_cols is NULL)', p_parent_table;
		END IF;

		IF v_jobmon THEN 
		    SELECT nspname INTO v_jobmon_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN DROP CONSTRAINT: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Entering constraint drop loop');
		    PERFORM update_step(v_step_id, 'OK', 'Done');
		END IF;


		FOREACH v_col IN ARRAY v_constraint_cols
		LOOP

		    SELECT c.conname
		    INTO v_existing_constraint_name
		    FROM pg_catalog.pg_constraint c 
		        JOIN pg_catalog.pg_attribute a ON c.conrelid = a.attrelid 
		    WHERE conrelid = p_child_table::regclass 
		        AND c.conname LIKE 'partmanconstr_%'
		        AND c.contype = 'c' 
		        AND a.attname = v_col
		        AND ARRAY[a.attnum] <@ c.conkey 
		        AND a.attisdropped = false;

		    IF v_existing_constraint_name IS NOT NULL THEN
		        v_exists := TRUE;
		        IF v_jobmon_schema IS NOT NULL THEN
		            v_step_id := add_step(v_job_id, 'Dropping constraint on column: '||v_col);
		        END IF;
		        v_sql := 'ALTER TABLE '||p_child_table||' DROP CONSTRAINT '||v_existing_constraint_name;
		        IF p_debug THEN
		            RAISE NOTICE 'Constraint drop query: %', v_sql;
		        END IF;
		        EXECUTE v_sql;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Drop constraint query: '||v_sql);
		        END IF;
		    END IF;

		END LOOP;

		IF v_jobmon_schema IS NOT NULL AND v_exists IS FALSE THEN
		    v_step_id := add_step(v_job_id, 'No constraints found to drop on child table: '||p_child_table);
		    PERFORM update_step(v_step_id, 'OK', 'Done');
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN DROP CONSTRAINT: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// drop_partition_id()
	_, err = tx.Exec(`
		/*
		 * Function to drop child tables from an id-based partition set. 
		 * Options to move table to different schema, drop only indexes or actually drop the table from the database.
		 */
		CREATE FUNCTION partman.drop_partition_id(p_parent_table text, p_retention bigint DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock                  boolean;
		v_child_table               text;
		v_control                   text;
		v_drop_count                int := 0;
		v_id_position               int;
		v_index                     record;
		v_job_id                    bigint;
		v_jobmon                    boolean;
		v_jobmon_schema             text;
		v_max                       bigint;
		v_old_search_path           text;
		v_part_interval             bigint;
		v_partition_id              bigint;
		v_retention                 bigint;
		v_retention_keep_index      boolean;
		v_retention_keep_table      boolean;
		v_retention_schema          text;
		v_step_id                   bigint;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman drop_partition_id'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'drop_partition_id already running.';
		    RETURN 0;
		END IF;

		-- Allow override of configuration options
		IF p_retention IS NULL THEN
		    SELECT  
		        part_interval::bigint
		        , control
		        , retention::bigint
		        , retention_keep_table
		        , retention_keep_index
		        , retention_schema
		        , jobmon
		    INTO
		        v_part_interval
		        , v_control
		        , v_retention
		        , v_retention_keep_table
		        , v_retention_keep_index
		        , v_retention_schema
		        , v_jobmon
		    FROM partman.part_config 
		    WHERE parent_table = p_parent_table 
		    AND (type = 'id-static' OR type = 'id-dynamic') 
		    AND retention IS NOT NULL;

		    IF v_part_interval IS NULL THEN
		        RAISE EXCEPTION 'Configuration for given parent table with a retention period not found: %', p_parent_table;
		    END IF;
		ELSE
		     SELECT  
		        part_interval::bigint
		        , control
		        , retention_keep_table
		        , retention_keep_index
		        , retention_schema
		        , jobmon
		    INTO
		        v_part_interval
		        , v_control
		        , v_retention_keep_table
		        , v_retention_keep_index
		        , v_retention_schema
		        , v_jobmon
		    FROM partman.part_config 
		    WHERE parent_table = p_parent_table 
		    AND (type = 'id-static' OR type = 'id-dynamic'); 
		    v_retention := p_retention;

		    IF v_part_interval IS NULL THEN
		        RAISE EXCEPTION 'Configuration for given parent table not found: %', p_parent_table;
		    END IF;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF p_keep_table IS NOT NULL THEN
		    v_retention_keep_table = p_keep_table;
		END IF;
		IF p_keep_index IS NOT NULL THEN
		    v_retention_keep_index = p_keep_index;
		END IF;
		IF p_retention_schema IS NOT NULL THEN
		    v_retention_schema = p_retention_schema;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN DROP ID PARTITION: '|| p_parent_table);
		END IF;

		EXECUTE 'SELECT max('||v_control||') FROM '||p_parent_table INTO v_max;

		-- Loop through child tables of the given parent
		FOR v_child_table IN 
		    SELECT n.nspname||'.'||c.relname FROM pg_inherits i join pg_class c ON i.inhrelid = c.oid join pg_namespace n ON c.relnamespace = n.oid WHERE i.inhparent::regclass = p_parent_table::regclass ORDER BY i.inhrelid ASC
		LOOP
		    v_id_position := (length(v_child_table) - position('p_' in reverse(v_child_table))) + 2;
		    v_partition_id := substring(v_child_table from v_id_position)::bigint;

		    -- Add one interval since partition names contain the start of the constraint period
		    IF v_retention <= (v_max - (v_partition_id + v_part_interval)) THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            v_step_id := add_step(v_job_id, 'Uninherit table '||v_child_table||' from '||p_parent_table);
		        END IF;
		        EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Done');
		        END IF;
		        IF v_retention_schema IS NULL THEN
		            IF v_retention_keep_table = false THEN
		                IF v_jobmon_schema IS NOT NULL THEN
		                    v_step_id := add_step(v_job_id, 'Drop table '||v_child_table);
		                END IF;
		                EXECUTE 'DROP TABLE '||v_child_table||' CASCADE';
		                IF v_jobmon_schema IS NOT NULL THEN
		                    PERFORM update_step(v_step_id, 'OK', 'Done');
		                END IF;
		            ELSIF v_retention_keep_index = false THEN
		                FOR v_index IN 
		                    SELECT i.indexrelid::regclass AS name
		                    , c.conname
		                    FROM pg_catalog.pg_index i
		                    LEFT JOIN pg_catalog.pg_constraint c ON i.indexrelid = c.conindid 
		                    WHERE i.indrelid = v_child_table::regclass
		                LOOP
		                    IF v_jobmon_schema IS NOT NULL THEN
		                        v_step_id := add_step(v_job_id, 'Drop index '||v_index.name||' from '||v_child_table);
		                    END IF;
		                    IF v_index.conname IS NOT NULL THEN
		                        EXECUTE 'ALTER TABLE '||v_child_table||' DROP CONSTRAINT '||v_index.conname;
		                    ELSE
		                        EXECUTE 'DROP INDEX '||v_index.name;
		                    END IF;
		                    IF v_jobmon_schema IS NOT NULL THEN
		                        PERFORM update_step(v_step_id, 'OK', 'Done');
		                    END IF;
		                END LOOP;
		            END IF;
		        ELSE -- Move to new schema
		            IF v_jobmon_schema IS NOT NULL THEN
		                v_step_id := add_step(v_job_id, 'Moving table '||v_child_table||' to schema '||v_retention_schema);
		            END IF;

		            EXECUTE 'ALTER TABLE '||v_child_table||' SET SCHEMA '||v_retention_schema; 

		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Done');
		            END IF;
		        END IF; -- End retention schema if

		        -- If child table is a subpartition, remove it from part_config & part_config_sub (should cascade due to FK)
		        DELETE FROM partman.part_config WHERE parent_table = v_child_table;

		        v_drop_count := v_drop_count + 1;
		    END IF; -- End retention check IF

		END LOOP; -- End child table loop

		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Finished partition drop maintenance');
		    PERFORM update_step(v_step_id, 'OK', v_drop_count||' partitions dropped.');
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_drop_count;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN DROP ID PARTITION: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// drop_partition_time()
	_, err = tx.Exec(`
		/*
		 * Function to drop child tables from a time-based partition set.
		 * Options to move table to different schema, drop only indexes or actually drop the table from the database.
		 */
		CREATE FUNCTION partman.drop_partition_time(p_parent_table text, p_retention interval DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock                  boolean;
		v_child_table               text;
		v_datetime_string           text;
		v_drop_count                int := 0;
		v_index                     record;
		v_job_id                    bigint;
		v_jobmon                    boolean;
		v_jobmon_schema             text;
		v_old_search_path           text;
		v_part_interval             interval;
		v_partition_timestamp       timestamp;
		v_quarter                   text;
		v_retention                 interval;
		v_retention_keep_index      boolean;
		v_retention_keep_table      boolean;
		v_retention_schema          text;
		v_step_id                   bigint;
		v_time_position             int;
		v_type                      text;
		v_year                      text;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman drop_partition_time'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'drop_partition_time already running.';
		    RETURN 0;
		END IF;

		-- Allow override of configuration options
		IF p_retention IS NULL THEN
		    SELECT  
		        type
		        , part_interval::interval
		        , retention::interval
		        , retention_keep_table
		        , retention_keep_index
		        , datetime_string
		        , retention_schema
		        , jobmon
		    INTO
		        v_type
		        , v_part_interval
		        , v_retention
		        , v_retention_keep_table
		        , v_retention_keep_index
		        , v_datetime_string
		        , v_retention_schema
		        , v_jobmon
		    FROM partman.part_config 
		    WHERE parent_table = p_parent_table 
		    AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom') 
		    AND retention IS NOT NULL;
		    
		    IF v_part_interval IS NULL THEN
		        RAISE EXCEPTION 'Configuration for given parent table with a retention period not found: %', p_parent_table;
		    END IF;
		ELSE
		    SELECT  
		        type
		        , part_interval::interval
		        , retention_keep_table
		        , retention_keep_index
		        , datetime_string
		        , retention_schema
		        , jobmon
		    INTO
		        v_type
		        , v_part_interval
		        , v_retention_keep_table
		        , v_retention_keep_index
		        , v_datetime_string
		        , v_retention_schema
		        , v_jobmon
		    FROM partman.part_config 
		    WHERE parent_table = p_parent_table 
		    AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom'); 
		    v_retention := p_retention;
		    
		    IF v_part_interval IS NULL THEN
		        RAISE EXCEPTION 'Configuration for given parent table not found: %', p_parent_table;
		    END IF;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF p_keep_table IS NOT NULL THEN
		    v_retention_keep_table = p_keep_table;
		END IF;
		IF p_keep_index IS NOT NULL THEN
		    v_retention_keep_index = p_keep_index;
		END IF;
		IF p_retention_schema IS NOT NULL THEN
		    v_retention_schema = p_retention_schema;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN DROP TIME PARTITION: '|| p_parent_table);
		END IF;

		-- Loop through child tables of the given parent
		FOR v_child_table IN 
		    SELECT n.nspname||'.'||c.relname FROM pg_inherits i join pg_class c ON i.inhrelid = c.oid join pg_namespace n ON c.relnamespace = n.oid WHERE i.inhparent::regclass = p_parent_table::regclass ORDER BY i.inhrelid ASC
		LOOP
		    -- pull out datetime portion of partition's tablename to make the next one
		    v_time_position := (length(v_child_table) - position('p_' in reverse(v_child_table))) + 2;
		    IF v_part_interval <> '3 months' OR (v_part_interval = '3 months' AND v_type = 'time-custom') THEN
		        v_partition_timestamp := to_timestamp(substring(v_child_table from v_time_position), v_datetime_string);
		    ELSE
		        -- to_timestamp doesn't recognize 'Q' date string formater. Handle it
		        v_year := split_part(substring(v_child_table from v_time_position), 'q', 1);
		        v_quarter := split_part(substring(v_child_table from v_time_position), 'q', 2);
		        CASE
		            WHEN v_quarter = '1' THEN
		                v_partition_timestamp := to_timestamp(v_year || '-01-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '2' THEN
		                v_partition_timestamp := to_timestamp(v_year || '-04-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '3' THEN
		                v_partition_timestamp := to_timestamp(v_year || '-07-01', 'YYYY-MM-DD');
		            WHEN v_quarter = '4' THEN
		                v_partition_timestamp := to_timestamp(v_year || '-10-01', 'YYYY-MM-DD');
		        END CASE;
		    END IF;

		    -- Add one interval since partition names contain the start of the constraint period
		    IF v_retention < (CURRENT_TIMESTAMP - (v_partition_timestamp + v_part_interval)) THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            v_step_id := add_step(v_job_id, 'Uninherit table '||v_child_table||' from '||p_parent_table);
		        END IF;
		        EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Done');
		        END IF;
		        IF v_retention_schema IS NULL THEN
		            IF v_retention_keep_table = false THEN
		                IF v_jobmon_schema IS NOT NULL THEN
		                    v_step_id := add_step(v_job_id, 'Drop table '||v_child_table);
		                END IF;
		                EXECUTE 'DROP TABLE '||v_child_table||' CASCADE';
		                IF v_jobmon_schema IS NOT NULL THEN
		                    PERFORM update_step(v_step_id, 'OK', 'Done');
		                END IF;
		            ELSIF v_retention_keep_index = false THEN
		                FOR v_index IN 
		                    SELECT i.indexrelid::regclass AS name
		                    , c.conname
		                    FROM pg_catalog.pg_index i
		                    LEFT JOIN pg_catalog.pg_constraint c ON i.indexrelid = c.conindid 
		                    WHERE i.indrelid = v_child_table::regclass
		                LOOP
		                    IF v_jobmon_schema IS NOT NULL THEN
		                        v_step_id := add_step(v_job_id, 'Drop index '||v_index.name||' from '||v_child_table);
		                    END IF;
		                    IF v_index.conname IS NOT NULL THEN
		                        EXECUTE 'ALTER TABLE '||v_child_table||' DROP CONSTRAINT '||v_index.conname;
		                    ELSE
		                        EXECUTE 'DROP INDEX '||v_index.name;
		                    END IF;
		                    IF v_jobmon_schema IS NOT NULL THEN
		                        PERFORM update_step(v_step_id, 'OK', 'Done');
		                    END IF;
		                END LOOP;
		            END IF;
		        ELSE -- Move to new schema
		            IF v_jobmon_schema IS NOT NULL THEN
		                v_step_id := add_step(v_job_id, 'Moving table '||v_child_table||' to schema '||v_retention_schema);
		            END IF;

		            EXECUTE 'ALTER TABLE '||v_child_table||' SET SCHEMA '||v_retention_schema; 

		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Done');
		            END IF;
		        END IF; -- End retention schema if

		        -- If child table is a subpartition, remove it from part_config & part_config_sub (should cascade due to FK)
		        DELETE FROM partman.part_config WHERE parent_table = v_child_table;

		        v_drop_count := v_drop_count + 1;
		    END IF; -- End retention check IF

		END LOOP; -- End child table loop

		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Finished partition drop maintenance');
		    PERFORM update_step(v_step_id, 'OK', v_drop_count||' partitions dropped.');
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_drop_count;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN DROP TIME PARTITION: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// partition_data_id()
	_, err = tx.Exec(`
		/*
		 * Populate the child table(s) of an id-based partition set with old data from the original parent
		 */
		CREATE FUNCTION partman.partition_data_id(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval int DEFAULT NULL, p_lock_wait numeric DEFAULT 0, p_order text DEFAULT 'ASC') RETURNS bigint
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_control                   text;
		v_current_partition_name    text;
		v_lock_iter                 int := 1;
		v_lock_obtained             boolean := FALSE;
		v_max_partition_id          bigint;
		v_min_partition_id          bigint;
		v_parent_schema             text;
		v_parent_tablename          text;
		v_part_interval             bigint;
		v_partition_id              bigint[];
		v_rowcount                  bigint;
		v_sql                       text;
		v_start_control             bigint;
		v_total_rows                bigint := 0;
		v_type                      text;

		BEGIN

		SELECT type
		    , part_interval::bigint
		    , control
		INTO v_type
		    , v_part_interval
		    , v_control
		FROM partman.part_config 
		WHERE parent_table = p_parent_table
		AND (type = 'id-static' OR type = 'id-dynamic');
		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		IF p_batch_interval IS NULL OR p_batch_interval > v_part_interval THEN
		    p_batch_interval := v_part_interval;
		END IF;

		FOR i IN 1..p_batch_count LOOP

		    IF p_order = 'ASC' THEN
		        EXECUTE 'SELECT min('||v_control||') FROM ONLY '||p_parent_table INTO v_start_control;
		        IF v_start_control IS NULL THEN
		            EXIT;
		        END IF;
		        v_min_partition_id = v_start_control - (v_start_control % v_part_interval);
		        v_partition_id := ARRAY[v_min_partition_id];
		        -- Check if custom batch interval overflows current partition maximum
		        IF (v_start_control + p_batch_interval) >= (v_min_partition_id + v_part_interval) THEN
		            v_max_partition_id := v_min_partition_id + v_part_interval;
		        ELSE
		            v_max_partition_id := v_start_control + p_batch_interval;
		        END IF;

		    ELSIF p_order = 'DESC' THEN
		        EXECUTE 'SELECT max('||v_control||') FROM ONLY '||p_parent_table INTO v_start_control;
		        IF v_start_control IS NULL THEN
		            EXIT;
		        END IF;
		        v_min_partition_id = v_start_control - (v_start_control % v_part_interval);
		        -- Must be greater than max value still in parent table since query below grabs < max
		        v_max_partition_id := v_min_partition_id + v_part_interval;
		        v_partition_id := ARRAY[v_min_partition_id];
		        -- Make sure minimum doesn't underflow current partition minimum
		        IF (v_start_control - p_batch_interval) >= v_min_partition_id THEN
		            v_min_partition_id = v_start_control - p_batch_interval;
		        END IF;
		    ELSE
		        RAISE EXCEPTION 'Invalid value for p_order. Must be ASC or DESC';
		    END IF;

		    -- do some locking with timeout, if required
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                v_sql := 'SELECT * FROM ONLY ' || p_parent_table ||
		                ' WHERE '||v_control||' >= '||quote_literal(v_min_partition_id)||
		                ' AND '||v_control||' < '||quote_literal(v_max_partition_id)
		                ||' FOR UPDATE NOWAIT';
		                EXECUTE v_sql;
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		           RETURN -1;
		        END IF;
		    END IF;

		    PERFORM partman.create_partition_id(p_parent_table, v_partition_id);
		    SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = p_parent_table;
		    v_current_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_min_partition_id::text, TRUE);

		    EXECUTE 'WITH partition_data AS (
		        DELETE FROM ONLY '||p_parent_table||' WHERE '||v_control||' >= '||v_min_partition_id||
		            ' AND '||v_control||' < '||v_max_partition_id||' RETURNING *)
		        INSERT INTO '||v_current_partition_name||' SELECT * FROM partition_data';        

		    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		    v_total_rows := v_total_rows + v_rowcount;
		    IF v_rowcount = 0 THEN
		        EXIT;
		    END IF;

		END LOOP;

		IF v_type = 'id-static' THEN
		        PERFORM partman.create_function_id(p_parent_table);
		END IF;

		RETURN v_total_rows;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// partition_data_time()
	_, err = tx.Exec(`
		/*
		 * Populate the child table(s) of a time-based partition set with old data from the original parent
		 */
		CREATE FUNCTION partman.partition_data_time(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval interval DEFAULT NULL, p_lock_wait numeric DEFAULT 0, p_order text DEFAULT 'ASC') RETURNS bigint
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_control                   text;
		v_datetime_string           text;
		v_current_partition_name       text;
		v_max_partition_timestamp   timestamp;
		v_last_partition            text;
		v_lock_iter                 int := 1;
		v_lock_obtained             boolean := FALSE;
		v_min_partition_timestamp   timestamp;
		v_parent_schema             text;
		v_parent_tablename          text;
		v_part_interval             interval;
		v_partition_suffix          text;
		v_partition_timestamp       timestamp[];
		v_quarter                   text;
		v_rowcount                  bigint;
		v_sql                       text;
		v_start_control             timestamp;
		v_time_position             int;
		v_total_rows                bigint := 0;
		v_type                      text;
		v_year                      text;

		BEGIN

		SELECT type
		    , part_interval::interval
		    , control
		    , datetime_string
		INTO v_type
		    , v_part_interval
		    , v_control
		    , v_datetime_string
		FROM partman.part_config 
		WHERE parent_table = p_parent_table
		AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom');
		IF NOT FOUND THEN
		    RAISE EXCEPTION 'ERROR: no config found for %', p_parent_table;
		END IF;

		IF p_batch_interval IS NULL OR p_batch_interval > v_part_interval THEN
		    p_batch_interval := v_part_interval;
		END IF;

		SELECT show_partitions INTO v_last_partition FROM partman.show_partitions(p_parent_table, 'DESC') LIMIT 1;

		FOR i IN 1..p_batch_count LOOP

		    IF p_order = 'ASC' THEN
		        EXECUTE 'SELECT min('||v_control||') FROM ONLY '||p_parent_table INTO v_start_control;
		    ELSIF p_order = 'DESC' THEN
		        EXECUTE 'SELECT max('||v_control||') FROM ONLY '||p_parent_table INTO v_start_control;
		    ELSE
		        RAISE EXCEPTION 'Invalid value for p_order. Must be ASC or DESC';
		    END IF;

		    IF v_start_control IS NULL THEN
		        EXIT;
		    END IF;

		    IF v_type = 'time-static' OR v_type = 'time-dynamic' THEN
		        CASE
		            WHEN v_part_interval = '15 mins' THEN
		                v_min_partition_timestamp := date_trunc('hour', v_start_control) + 
		                    '15min'::interval * floor(date_part('minute', v_start_control) / 15.0);
		            WHEN v_part_interval = '30 mins' THEN
		                v_min_partition_timestamp := date_trunc('hour', v_start_control) + 
		                    '30min'::interval * floor(date_part('minute', v_start_control) / 30.0);
		            WHEN v_part_interval = '1 hour' THEN
		                v_min_partition_timestamp := date_trunc('hour', v_start_control);
		            WHEN v_part_interval = '1 day' THEN
		                v_min_partition_timestamp := date_trunc('day', v_start_control);
		            WHEN v_part_interval = '1 week' THEN
		                v_min_partition_timestamp := date_trunc('week', v_start_control);
		            WHEN v_part_interval = '1 month' THEN
		                v_min_partition_timestamp := date_trunc('month', v_start_control);
		            WHEN v_part_interval = '3 months' THEN
		                v_min_partition_timestamp := date_trunc('quarter', v_start_control);
		            WHEN v_part_interval = '1 year' THEN
		                v_min_partition_timestamp := date_trunc('year', v_start_control);
		        END CASE;
		    ELSIF v_type = 'time-custom' THEN
		        -- Keep going backwards, checking if the time interval encompases the current v_start_control value
		        v_time_position := (length(v_last_partition) - position('p_' in reverse(v_last_partition))) + 2;
		        v_min_partition_timestamp := to_timestamp(substring(v_last_partition from v_time_position), v_datetime_string);
		        v_max_partition_timestamp := v_min_partition_timestamp + v_part_interval;
		        LOOP
		            IF v_start_control >= v_min_partition_timestamp AND v_start_control < v_max_partition_timestamp THEN
		                EXIT;
		            ELSE
		                v_max_partition_timestamp := v_min_partition_timestamp;
		                BEGIN
		                    v_min_partition_timestamp := v_min_partition_timestamp - v_part_interval;
		                EXCEPTION WHEN datetime_field_overflow THEN
		                    RAISE EXCEPTION 'Attempted partition time interval is outside PostgreSQL''s supported time range. 
		                        Unable to create partition with interval before timestamp % ', v_min_partition_interval;
		                END;
		            END IF;
		        END LOOP;

		    END IF;

		    v_partition_timestamp := ARRAY[v_min_partition_timestamp];
		    IF p_order = 'ASC' THEN
		        IF (v_start_control + p_batch_interval) >= (v_min_partition_timestamp + v_part_interval) THEN
		            v_max_partition_timestamp := v_min_partition_timestamp + v_part_interval;
		        ELSE
		            v_max_partition_timestamp := v_start_control + p_batch_interval;
		        END IF;
		    ELSIF p_order = 'DESC' THEN
		        -- Must be greater than max value still in parent table since query below grabs < max
		        v_max_partition_timestamp := v_min_partition_timestamp + v_part_interval;
		        -- Make sure minimum doesn't underflow current partition minimum
		        IF (v_start_control - p_batch_interval) >= v_min_partition_timestamp THEN
		            v_min_partition_timestamp = v_start_control - p_batch_interval;
		        END IF;
		    ELSE
		        RAISE EXCEPTION 'Invalid value for p_order. Must be ASC or DESC';
		    END IF;

		-- do some locking with timeout, if required
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                v_sql := 'SELECT * FROM ONLY ' || p_parent_table ||
		                ' WHERE '||v_control||' >= '||quote_literal(v_min_partition_timestamp)||
		                ' AND '||v_control||' < '||quote_literal(v_max_partition_timestamp)
		                ||' FOR UPDATE NOWAIT';
		                EXECUTE v_sql;
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		           RETURN -1;
		        END IF;
		    END IF;

		    PERFORM partman.create_partition_time(p_parent_table, v_partition_timestamp);
		    -- This suffix generation code is in create_partition_time() as well
		    v_partition_suffix := to_char(v_min_partition_timestamp, 'YYYY');
		    IF v_part_interval < '1 year' AND v_part_interval <> '1 week' THEN 
		        v_partition_suffix := v_partition_suffix ||'_'|| to_char(v_min_partition_timestamp, 'MM');
		        IF v_part_interval < '1 month' AND v_part_interval <> '1 week' THEN 
		            v_partition_suffix := v_partition_suffix ||'_'|| to_char(v_min_partition_timestamp, 'DD');
		            IF v_part_interval < '1 day' THEN
		                v_partition_suffix := v_partition_suffix || '_' || to_char(v_min_partition_timestamp, 'HH24MI');
		                IF v_part_interval < '1 minute' THEN
		                    v_partition_suffix := v_partition_suffix || to_char(v_min_partition_timestamp, 'SS');
		                END IF; -- end < minute IF
		            END IF; -- end < day IF      
		        END IF; -- end < month IF
		    END IF; -- end < year IF
		    IF v_part_interval = '1 week' THEN
		        v_partition_suffix := to_char(v_min_partition_timestamp, 'IYYY') || 'w' || to_char(v_min_partition_timestamp, 'IW');
		    END IF;
		    -- "Q" is ignored in to_timestamp, so handle special case
		    IF v_part_interval = '3 months' AND (v_type = 'time-static' OR v_type = 'time-dynamic') THEN
		        v_year := to_char(v_min_partition_timestamp, 'YYYY');
		        v_quarter := to_char(v_min_partition_timestamp, 'Q');
		        v_partition_suffix := v_year || 'q' || v_quarter;
		    END IF;

		    SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = p_parent_table;
		    v_current_partition_name := partman.check_name_length(v_parent_tablename, v_parent_schema, v_partition_suffix, TRUE);

		    EXECUTE 'WITH partition_data AS (
		            DELETE FROM ONLY '||p_parent_table||' WHERE '||v_control||' >= '||quote_literal(v_min_partition_timestamp)||
		                ' AND '||v_control||' < '||quote_literal(v_max_partition_timestamp)||' RETURNING *)
		            INSERT INTO '||v_current_partition_name||' SELECT * FROM partition_data';

		    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		    v_total_rows := v_total_rows + v_rowcount;
		    IF v_rowcount = 0 THEN
		        EXIT;
		    END IF;

		END LOOP; 

		IF v_type = 'time-static' THEN
		        PERFORM partman.create_function_time(p_parent_table);
		END IF;

		RETURN v_total_rows;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// reapply_privileges()
	_, err = tx.Exec(`
		/*
		 * Function to re-apply ownership & privileges on all child tables in a partition set using parent table as reference
		 */
		CREATE FUNCTION partman.reapply_privileges(p_parent_table text) RETURNS void
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_all               text[] := ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'];
		v_child_owner       text;
		v_child_table       text;
		v_child_grant       record;
		v_grant             text;
		v_grantees          text[];
		v_job_id            bigint;
		v_jobmon            boolean;
		v_jobmon_schema     text;
		v_match             boolean;
		v_old_search_path   text;
		v_parent_owner      text;
		v_owner_sql         text;
		v_revoke            text[];
		v_parent_grant      record;
		v_sql               text;
		v_step_id           bigint;

		BEGIN

		SELECT jobmon INTO v_jobmon FROM partman.part_config WHERE parent_table = p_parent_table;
		IF v_jobmon IS NULL THEN
		    RAISE EXCEPTION 'Given table is not managed by this extention: %', p_parent_table;
		END IF;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN RE-APPLYING PRIVILEGES TO ALL CHILD TABLES OF: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Setting new child table privileges');
		END IF;

		SELECT tableowner INTO v_parent_owner FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;

		FOR v_child_table IN 
		    SELECT n.nspname||'.'||c.relname FROM pg_inherits i join pg_class c ON i.inhrelid = c.oid join pg_namespace n ON c.relnamespace = n.oid WHERE i.inhparent::regclass = p_parent_table::regclass ORDER BY i.inhrelid ASC
		LOOP
		    IF v_jobmon_schema IS NOT NULL THEN
		        PERFORM update_step(v_step_id, 'PENDING', 'Currently on child partition in ascending order: '||v_child_table);
		    END IF;
		    v_grantees := NULL;
		    FOR v_parent_grant IN 
		        SELECT array_agg(DISTINCT privilege_type::text ORDER BY privilege_type::text) AS types, grantee
		        FROM information_schema.table_privileges 
		        WHERE table_schema ||'.'|| table_name = p_parent_table
		        GROUP BY grantee 
		    LOOP
		        -- Compare parent & child grants. Don't re-apply if it already exists
		        v_match := false;
		        FOR v_child_grant IN 
		            SELECT array_agg(DISTINCT privilege_type::text ORDER BY privilege_type::text) AS types, grantee
		            FROM information_schema.table_privileges 
		            WHERE table_schema ||'.'|| table_name = v_child_table
		            GROUP BY grantee 
		        LOOP
		            IF v_parent_grant.types = v_child_grant.types AND v_parent_grant.grantee = v_child_grant.grantee THEN
		                v_match := true;
		            END IF;
		        END LOOP;

		        IF v_match = false THEN
		            EXECUTE 'GRANT '||array_to_string(v_parent_grant.types, ',')||' ON '||v_child_table||' TO '||v_parent_grant.grantee;
		            SELECT array_agg(r) INTO v_revoke FROM (SELECT unnest(v_all) AS r EXCEPT SELECT unnest(v_parent_grant.types)) x;
		            IF v_revoke IS NOT NULL THEN
		                EXECUTE 'REVOKE '||array_to_string(v_revoke, ',')||' ON '||v_child_table||' FROM '||v_parent_grant.grantee||' CASCADE';
		            END IF;
		        END IF;

		        v_grantees := array_append(v_grantees, v_parent_grant.grantee::text);

		    END LOOP;
		    
		    -- Revoke all privileges from roles that have none on the parent
		    IF v_grantees IS NOT NULL THEN
		        SELECT array_agg(r) INTO v_revoke FROM (
		            SELECT DISTINCT grantee::text AS r FROM information_schema.table_privileges WHERE table_schema ||'.'|| table_name = v_child_table
		            EXCEPT
		            SELECT unnest(v_grantees)) x;
		        IF v_revoke IS NOT NULL THEN
		            EXECUTE 'REVOKE ALL ON '||v_child_table||' FROM '||array_to_string(v_revoke, ',');
		        END IF;
		    END IF;

		    SELECT tableowner INTO v_child_owner FROM pg_tables WHERE schemaname ||'.'|| tablename = v_child_table;
		    IF v_parent_owner <> v_child_owner THEN
		        EXECUTE 'ALTER TABLE '||v_child_table||' OWNER TO '||v_parent_owner;
		    END IF;

		END LOOP;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM update_step(v_step_id, 'OK', 'Done');
		    PERFORM close_job(v_job_id);
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN RE-APPLYING PRIVILEGES TO ALL CHILD TABLES OF: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// run_maintenance()
	_, err = tx.Exec(`
		/*
		 * Function to manage pre-creation of the next partitions in a set.
		 * Also manages dropping old partitions if the retention option is set.
		 * If p_parent_table is passed, will only run run_maintenance() on that one table (no matter what the configuration table may have set for it)
		 * Otherwise, will run on all tables in the config table with p_run_maintenance() set to true.
		 * For large partition sets, running analyze can cause maintenance to take longer than expected. Can set p_analyze to false to avoid a forced analyze run.
		 * Be aware that constraint exclusion may not work properly until an analyze on the partition set is run. 
		 */
		CREATE FUNCTION partman.run_maintenance(p_parent_table text DEFAULT NULL, p_analyze boolean DEFAULT true, p_jobmon boolean DEFAULT true) RETURNS void 
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock                      boolean;
		v_create_count                  int := 0;
		v_current_partition             text;
		v_current_partition_id          bigint;
		v_current_partition_timestamp   timestamp;
		v_datetime_string               text;
		v_drop_count                    int := 0;
		v_id_position                   int;
		v_job_id                        bigint;
		v_jobmon                        boolean;
		v_jobmon_schema                 text;
		v_last_partition                text;
		v_last_partition_created        boolean;
		v_last_partition_id             bigint;
		v_last_partition_timestamp      timestamp;
		v_next_partition_id             bigint;
		v_next_partition_timestamp      timestamp;
		v_old_search_path               text;
		v_premade_count                 int;
		v_quarter                       text;
		v_step_id                       bigint;
		v_step_overflow_id              bigint;
		v_step_serial_id                bigint;
		v_sub_parent                    text;
		v_row                           record;
		v_row_sub                       record;
		v_tablename                     text;
		v_tables_list_sql               text;
		v_time_position                 int;
		v_year                          text;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman run_maintenance'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'Partman maintenance already running.';
		    RETURN;
		END IF;

		IF p_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN RUN MAINTENANCE');
		    v_step_id := add_step(v_job_id, 'Running maintenance loop');
		END IF;

		v_tables_list_sql := 'SELECT parent_table
		                , type
		                , part_interval
		                , control
		                , premake
		                , datetime_string
		                , undo_in_progress
		            FROM partman.part_config';

		IF p_parent_table IS NULL THEN
		    v_tables_list_sql := v_tables_list_sql || ' WHERE use_run_maintenance = true';
		ELSE
		    v_tables_list_sql := v_tables_list_sql || format(' WHERE parent_table = %L', p_parent_table);
		END IF;

		FOR v_row IN EXECUTE v_tables_list_sql
		LOOP

		    CONTINUE WHEN v_row.undo_in_progress;

		    SELECT show_partitions INTO v_last_partition FROM partman.show_partitions(v_row.parent_table, 'DESC') LIMIT 1;

		    IF v_row.type = 'time-static' OR v_row.type = 'time-dynamic' OR v_row.type = 'time-custom' THEN

		        IF v_row.type = 'time-static' OR v_row.type = 'time-dynamic' THEN
		            CASE
		                WHEN v_row.part_interval::interval = '15 mins' THEN
		                    v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP) + 
		                        '15min'::interval * floor(date_part('minute', CURRENT_TIMESTAMP) / 15.0);
		                WHEN v_row.part_interval::interval = '30 mins' THEN
		                    v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP) + 
		                        '30min'::interval * floor(date_part('minute', CURRENT_TIMESTAMP) / 30.0);
		                WHEN v_row.part_interval::interval = '1 hour' THEN
		                    v_current_partition_timestamp := date_trunc('hour', CURRENT_TIMESTAMP);
		                 WHEN v_row.part_interval::interval = '1 day' THEN
		                    v_current_partition_timestamp := date_trunc('day', CURRENT_TIMESTAMP);
		                WHEN v_row.part_interval::interval = '1 week' THEN
		                    v_current_partition_timestamp := date_trunc('week', CURRENT_TIMESTAMP);
		                WHEN v_row.part_interval::interval = '1 month' THEN
		                    v_current_partition_timestamp := date_trunc('month', CURRENT_TIMESTAMP);
		                WHEN v_row.part_interval::interval = '3 months' THEN
		                    v_current_partition_timestamp := date_trunc('quarter', CURRENT_TIMESTAMP);
		                WHEN v_row.part_interval::interval = '1 year' THEN
		                    v_current_partition_timestamp := date_trunc('year', CURRENT_TIMESTAMP);
		            END CASE;
		        ELSIF v_row.type = 'time-custom' THEN
		            SELECT child_table INTO v_current_partition FROM partman.custom_time_partitions 
		                WHERE parent_table = v_row.parent_table AND partition_range @> CURRENT_TIMESTAMP;
		            IF v_current_partition IS NULL THEN
		                RAISE EXCEPTION 'Current time partition missing from custom_time_partitions config table for table % and timestamp %',
		                     CURRENT_TIMESTAMP, v_row.parent_table;
		            END IF;
		            v_time_position := (length(v_current_partition) - position('p_' in reverse(v_current_partition))) + 2;
		            v_current_partition_timestamp := to_timestamp(substring(v_current_partition from v_time_position), v_row.datetime_string);
		        END IF;

		        v_time_position := (length(v_last_partition) - position('p_' in reverse(v_last_partition))) + 2;
		        IF v_row.part_interval::interval <> '3 months' OR (v_row.part_interval::interval = '3 months' AND v_row.type = 'time-custom') THEN
		           v_last_partition_timestamp := to_timestamp(substring(v_last_partition from v_time_position), v_row.datetime_string);
		        ELSE
		            -- to_timestamp doesn't recognize 'Q' date string formater. Handle it
		            v_year := split_part(substring(v_last_partition from v_time_position), 'q', 1);
		            v_quarter := split_part(substring(v_last_partition from v_time_position), 'q', 2);
		            CASE
		                WHEN v_quarter = '1' THEN
		                    v_last_partition_timestamp := to_timestamp(v_year || '-01-01', 'YYYY-MM-DD');
		                WHEN v_quarter = '2' THEN
		                    v_last_partition_timestamp := to_timestamp(v_year || '-04-01', 'YYYY-MM-DD');
		                WHEN v_quarter = '3' THEN
		                    v_last_partition_timestamp := to_timestamp(v_year || '-07-01', 'YYYY-MM-DD');
		                WHEN v_quarter = '4' THEN
		                    v_last_partition_timestamp := to_timestamp(v_year || '-10-01', 'YYYY-MM-DD');
		            END CASE;
		        END IF;

		        -- Check and see how many premade partitions there are.
		        -- Can be negative when subpartitioning and there are parent partitions in the past compared to current timestamp value.
		        -- abs() prevents run_maintenence from running on those old parent tables
		        v_premade_count = abs(round(EXTRACT('epoch' FROM age(v_last_partition_timestamp, v_current_partition_timestamp)) / EXTRACT('epoch' FROM v_row.part_interval::interval)));
		        v_next_partition_timestamp := v_last_partition_timestamp;
		        -- Loop premaking until config setting is met. Allows it to catch up if it fell behind or if premake changed.
		        WHILE v_premade_count < v_row.premake LOOP
		            BEGIN
		                v_next_partition_timestamp := v_next_partition_timestamp + v_row.part_interval::interval;
		            EXCEPTION WHEN datetime_field_overflow THEN
		                v_premade_count := v_row.premake; -- do this so it can exit the premake check loop and continue in the outer for loop 
		                IF v_jobmon_schema IS NOT NULL THEN
		                    v_step_overflow_id := add_step(v_job_id, 'Attempted partition time interval is outside PostgreSQL''s supported time range.');
		                    PERFORM update_step(v_step_overflow_id, 'CRITICAL', 'Child partition creation skippd for parent table '||v_partition_time);
		                END IF;
		                RAISE WARNING 'Attempted partition time interval is outside PostgreSQL''s supported time range. Child partition creation skipped for parent table %', v_row.parent_table;
		                CONTINUE;
		            END;

		            v_last_partition_created := partman.create_partition_time(v_row.parent_table, ARRAY[v_next_partition_timestamp], p_analyze); 
		            v_create_count := v_create_count + 1;
		            IF v_row.type = 'time-static' AND v_last_partition_created THEN
		                PERFORM partman.create_function_time(v_row.parent_table);
		            END IF;

		            -- Manage additonal constraints if set
		            PERFORM partman.apply_constraints(v_row.parent_table);
		            -- Can be negative when subpartitioning and there are parent partitions in the past compared to current timestamp value.
		            -- abs() prevents run_maintenence from running on those old parent tables
		            v_premade_count = abs(round(EXTRACT('epoch' FROM age(v_next_partition_timestamp, v_current_partition_timestamp)) / EXTRACT('epoch' FROM v_row.part_interval::interval)));
		        END LOOP;
		    ELSIF v_row.type = 'id-static' OR v_row.type ='id-dynamic' THEN
		        -- This doesn't need the overall max of a full subpartition set, just the max of the current partition set
		        EXECUTE 'SELECT '||v_row.control||' - ('||v_row.control||' % '||v_row.part_interval::int||') FROM '||v_row.parent_table||'
		            WHERE '||v_row.control||' = (SELECT max('||v_row.control||') FROM '||v_row.parent_table||')' 
		            INTO v_current_partition_id;
		        v_id_position := (length(v_last_partition) - position('p_' in reverse(v_last_partition))) + 2;
		        v_last_partition_id = substring(v_last_partition from v_id_position)::bigint;
		        -- This catches if there's invalid data in a parent table set that's outside all child table ranges.
		        IF v_last_partition_id < v_current_partition_id THEN
		            IF v_jobmon_schema IS NOT NULL THEN
		                v_step_serial_id := add_step(v_job_id, 'Found inconsistent data in serial partition set.');
		                PERFORM update_step(v_step_serial_id, 'CRITICAL', 'Child partition creation skipped for parent table '||v_row.parent_table||'. Current max serial id value ('||v_current_partition_id||') is greater than the id range covered by the last partition created ('||v_last_partition||'). Run check_parent() to find possible cause.');
		            END IF;
		            RAISE WARNING 'Child partition creation skipped for parent table %. Found inconsistent data in serial partition set. Current max serial id value (%) is greater than the id range covered by the last partition created (%). Run check_parent() to find possible cause.', v_row.parent_table, v_current_partition_id, v_last_partition;
		            CONTINUE;
		        END IF;
		        v_next_partition_id := v_last_partition_id + v_row.part_interval::bigint;
		        -- Can be negative when subpartitioning and there are parent partitions with lower values compared to current id value.
		        -- abs() prevents run_maintenence from running on those old parent tables
		        WHILE (abs((v_next_partition_id - v_current_partition_id) / v_row.part_interval::bigint)) <= v_row.premake 
		        LOOP 
		            v_last_partition_created := partman.create_partition_id(v_row.parent_table, ARRAY[v_next_partition_id], p_analyze);
		            IF v_last_partition_created THEN
		                PERFORM partman.create_function_id(v_row.parent_table);
		                PERFORM partman.apply_constraints(v_row.parent_table);
		            END IF;
		            v_next_partition_id := v_next_partition_id + v_row.part_interval::bigint;
		        END LOOP;

		    END IF; -- end main IF check for time or id

		END LOOP; -- end of creation loop

		-- Manage dropping old partitions if retention option is set
		FOR v_row IN 
		    SELECT parent_table FROM partman.part_config WHERE retention IS NOT NULL AND undo_in_progress = false AND 
		        (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom')
		LOOP
		    IF p_parent_table IS NULL THEN
		        v_drop_count := v_drop_count + partman.drop_partition_time(v_row.parent_table);   
		    ELSE -- Only run retention on table given in parameter
		        IF p_parent_table <> v_row.parent_table THEN
		            CONTINUE;
		        ELSE
		            v_drop_count := v_drop_count + partman.drop_partition_time(v_row.parent_table);   
		        END IF;
		    END IF;
		END LOOP; 
		FOR v_row IN 
		    SELECT parent_table FROM partman.part_config WHERE retention IS NOT NULL AND undo_in_progress = false AND (type = 'id-static' OR type = 'id-dynamic')
		LOOP
		    IF p_parent_table IS NULL THEN
		        v_drop_count := v_drop_count + partman.drop_partition_id(v_row.parent_table);
		    ELSE -- Only run retention on table given in parameter
		        IF p_parent_table <> v_row.parent_table THEN
		            CONTINUE;
		        ELSE
		            v_drop_count := v_drop_count + partman.drop_partition_id(v_row.parent_table);
		        END IF;
		    END IF;
		END LOOP; 

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM update_step(v_step_id, 'OK', 'Partition maintenance finished. '||v_create_count||' partitons made. '||v_drop_count||' partitions dropped.');
		    IF v_step_overflow_id IS NOT NULL OR v_step_serial_id IS NOT NULL THEN
		        PERFORM fail_job(v_job_id);
		    ELSE
		        PERFORM close_job(v_job_id);
		    END IF;
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN RUN MAINTENANCE'')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// show_partitions()
	_, err = tx.Exec(`
		/*
		 * Function to list all child partitions in a set.
		 */
		CREATE FUNCTION partman.show_partitions (p_parent_table text, p_order text DEFAULT 'ASC') RETURNS SETOF text
		    LANGUAGE plpgsql STABLE SECURITY DEFINER 
		    AS $$
		DECLARE

		v_datetime_string   text;
		v_part_interval     text;  
		v_type              text;

		BEGIN

		IF p_order NOT IN ('ASC', 'DESC') THEN
		    RAISE EXCEPTION 'p_order paramter must be one of the following values: ASC, DESC';
		END IF;

		SELECT type
		    , part_interval
		    , datetime_string
		INTO v_type
		    , v_part_interval
		    , v_datetime_string
		FROM partman.part_config
		WHERE parent_table = p_parent_table;

		IF v_type IN ('time-static', 'time-dynamic', 'time-custom') THEN

		    RETURN QUERY EXECUTE '
		    SELECT n.nspname::text ||''.''|| c.relname::text AS partition_name FROM
		    pg_catalog.pg_inherits h
		    JOIN pg_catalog.pg_class c ON c.oid = h.inhrelid
		    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		    WHERE h.inhparent = '||quote_literal(p_parent_table)||'::regclass
		    ORDER BY to_timestamp(substring(c.relname from ((length(c.relname) - position(''p_'' in reverse(c.relname))) + 2) ), '||quote_literal(v_datetime_string)||') ' || p_order;

		ELSIF v_type IN ('id-static', 'id-dynamic') THEN
		    
		    RETURN QUERY EXECUTE '
		    SELECT n.nspname::text ||''.''|| c.relname::text AS partition_name FROM
		    pg_catalog.pg_inherits h
		    JOIN pg_catalog.pg_class c ON c.oid = h.inhrelid
		    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		    WHERE h.inhparent = '||quote_literal(p_parent_table)||'::regclass
		    ORDER BY substring(c.relname from ((length(c.relname) - position(''p_'' in reverse(c.relname))) + 2) )::bigint ' || p_order;

		END IF;

		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// undo_partition()
	_, err = tx.Exec(`
		/*
		 * Function to undo partitioning. 
		 * Will actually work on any parent/child table set, not just ones created by pg_partman.
		 */
		CREATE FUNCTION partman.undo_partition(p_parent_table text, p_batch_count int DEFAULT 1, p_keep_table boolean DEFAULT true, p_jobmon boolean DEFAULT true, p_lock_wait numeric DEFAULT 0) RETURNS bigint
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock              boolean;
		v_batch_loop_count      bigint := 0;
		v_child_count           bigint;
		v_child_table           text;
		v_copy_sql              text;
		v_function_name         text;
		v_job_id                bigint;
		v_jobmon_schema         text;
		v_lock_iter             int := 1;
		v_lock_obtained         boolean := FALSE;
		v_old_search_path       text;
		v_parent_schema         text;
		v_parent_tablename      text;
		v_part_interval         interval;
		v_rowcount              bigint;
		v_step_id               bigint;
		v_total                 bigint := 0;
		v_trig_name             text;
		v_undo_count            int := 0;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman undo_partition'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'undo_partition already running.';
		    RETURN 0;
		END IF;

		IF p_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN UNDO PARTITIONING: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Undoing partitioning for table '||p_parent_table);
		END IF;

		-- Stops new time partitons from being made as well as stopping child tables from being dropped if they were configured with a retention period.
		UPDATE partman.part_config SET undo_in_progress = true WHERE parent_table = p_parent_table;
		-- Stop data going into child tables and stop new id partitions from being made.
		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;
		v_trig_name := partman.check_name_length(p_object_name := v_parent_tablename, p_suffix := '_part_trig'); 
		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);

		SELECT tgname INTO v_trig_name FROM pg_catalog.pg_trigger t WHERE tgname = v_trig_name;
		IF v_trig_name IS NOT NULL THEN
		    -- lockwait for trigger drop
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                EXECUTE 'LOCK TABLE ONLY '||p_parent_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		            RAISE NOTICE 'Unable to obtain lock on parent table to remove trigger';
		            RETURN -1;
		        END IF;
		    END IF; -- END p_lock_wait IF
		    EXECUTE 'DROP TRIGGER IF EXISTS '||v_trig_name||' ON '||p_parent_table;
		END IF; -- END trigger IF
		v_lock_obtained := FALSE; -- reset for reuse later

		EXECUTE 'DROP FUNCTION IF EXISTS '||v_function_name||'()';

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM update_step(v_step_id, 'OK', 'Stopped partition creation process. Removed trigger & trigger function');
		END IF;

		WHILE v_batch_loop_count < p_batch_count LOOP 
		    SELECT n.nspname||'.'||c.relname INTO v_child_table
		    FROM pg_inherits i 
		    JOIN pg_class c ON i.inhrelid = c.oid 
		    JOIN pg_namespace n ON c.relnamespace = n.oid 
		    WHERE i.inhparent::regclass = p_parent_table::regclass 
		    ORDER BY i.inhrelid ASC;

		    EXIT WHEN v_child_table IS NULL;

		    EXECUTE 'SELECT count(*) FROM '||v_child_table INTO v_child_count;
		    IF v_child_count = 0 THEN
		        -- No rows left in this child table. Remove from partition set.

		        -- lockwait timeout for table drop
		        IF p_lock_wait > 0  THEN
		            v_lock_iter := 0;
		            WHILE v_lock_iter <= 5 LOOP
		                v_lock_iter := v_lock_iter + 1;
		                BEGIN
		                    EXECUTE 'LOCK TABLE ONLY '||v_child_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                    v_lock_obtained := TRUE;
		                EXCEPTION
		                    WHEN lock_not_available THEN
		                        PERFORM pg_sleep( p_lock_wait / 5.0 );
		                        CONTINUE;
		                END;
		                EXIT WHEN v_lock_obtained;
		            END LOOP;
		            IF NOT v_lock_obtained THEN
		                RAISE NOTICE 'Unable to obtain lock on child table for removal from partition set';
		                RETURN -1;
		            END IF;
		        END IF; -- END p_lock_wait IF
		        v_lock_obtained := FALSE; -- reset for reuse later

		        EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		        IF p_keep_table = false THEN
		            EXECUTE 'DROP TABLE '||v_child_table;
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table DROPPED. Moved '||coalesce(v_rowcount, 0)||' rows to parent');
		            END IF;
		        ELSE
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table UNINHERITED, not DROPPED. Copied '||coalesce(v_rowcount, 0)||' rows to parent');
		            END IF;
		        END IF;
		        v_undo_count := v_undo_count + 1;
		        CONTINUE;
		    END IF;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing child partition: '||v_child_table);
		    END IF;
		   
		    -- do some locking with timeout, if required
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                EXECUTE 'SELECT * FROM '|| v_child_table ||' FOR UPDATE NOWAIT';
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		           RAISE NOTICE 'Unable to obtain lock on batch of rows to move';
		           RETURN -1;
		        END IF;
		    END IF;

		    v_copy_sql := 'INSERT INTO '||p_parent_table||' SELECT * FROM '||v_child_table;
		    EXECUTE v_copy_sql;
		    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		    v_total := v_total + v_rowcount;

		    EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		    IF p_keep_table = false THEN
		        EXECUTE 'DROP TABLE '||v_child_table;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Child table DROPPED. Moved '||v_rowcount||' rows to parent');
		        END IF;
		    ELSE
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Child table UNINHERITED, not DROPPED. Copied '||v_rowcount||' rows to parent');
		        END IF;
		    END IF;
		    v_batch_loop_count := v_batch_loop_count + 1;
		    v_undo_count := v_undo_count + 1;         
		END LOOP;

		IF v_undo_count = 0 THEN
		    -- FOR loop never ran, so there's no child tables left.
		    DELETE FROM partman.part_config WHERE parent_table = p_parent_table;
		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing config from pg_partman (if it existed)');
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;
		END IF;

		RAISE NOTICE 'Copied % row(s) from % child table(s) to the parent: %', v_total, v_undo_count, p_parent_table;
		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Final stats');
		    PERFORM update_step(v_step_id, 'OK', 'Copied '||v_total||' row(s) from '||v_undo_count||' child table(s) to the parent');
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_total;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN UNDO PARTITIONING: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''Partition function maintenance for table '||p_parent_table||' failed'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// undo_partition_id()
	_, err = tx.Exec(`
		/*
		 * Function to undo id-based partitioning created by this extension
		 */
		CREATE FUNCTION partman.undo_partition_id(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval bigint DEFAULT NULL, p_keep_table boolean DEFAULT true, p_lock_wait numeric DEFAULT 0) RETURNS bigint
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock              boolean;
		v_batch_loop_count      int := 0;
		v_child_loop_total      bigint := 0;
		v_child_min             bigint;
		v_child_table           text;
		v_control               text;
		v_exists                int;
		v_function_name         text;
		v_inner_loop_count      int;
		v_job_id                bigint;
		v_jobmon                boolean;
		v_jobmon_schema         text;
		v_lock_iter             int := 1;
		v_lock_obtained         boolean := FALSE;
		v_move_sql              text;
		v_old_search_path       text;
		v_parent_schema         text;
		v_parent_tablename      text;
		v_part_interval         bigint;
		v_row                   record;
		v_rowcount              bigint;
		v_step_id               bigint;
		v_sub_count             int;
		v_trig_name             text;
		v_total                 bigint := 0;
		v_undo_count            int := 0;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman undo_id_partition'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'undo_id_partition already running.';
		    RETURN 0;
		END IF;

		SELECT part_interval::bigint
		    , control
		    , jobmon
		INTO v_part_interval
		    , v_control
		    , v_jobmon
		FROM partman.part_config 
		WHERE parent_table = p_parent_table 
		AND (type = 'id-static' OR type = 'id-dynamic');

		IF v_part_interval IS NULL THEN
		    RAISE EXCEPTION 'Configuration for given parent table not found: %', p_parent_table;
		END IF;

		-- Check if any child tables are themselves partitioned or part of an inheritance tree. Prevent undo at this level if so.
		-- Need to either lock child tables at all levels or handle the proper removal of triggers on all child tables first 
		--  before multi-level undo can be performed safely.
		FOR v_row IN 
		    SELECT show_partitions AS child_table FROM partman.show_partitions(p_parent_table)
		LOOP
		    SELECT count(*) INTO v_sub_count 
		    FROM pg_catalog.pg_inherits
		    WHERE inhparent::regclass = v_row.child_table::regclass;
		    IF v_sub_count > 0 THEN
		        RAISE EXCEPTION 'Child table for this parent has child table(s) itself (%). Run undo partitioning on this table or remove inheritance first to ensure all data is properly moved to parent', v_row.child_table;
		    END IF;
		END LOOP;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN UNDO PARTITIONING: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Undoing partitioning for table '||p_parent_table);
		END IF;

		IF p_batch_interval IS NULL THEN
		    p_batch_interval := v_part_interval;
		END IF;

		-- Stops new time partitons from being made as well as stopping child tables from being dropped if they were configured with a retention period.
		UPDATE partman.part_config SET undo_in_progress = true WHERE parent_table = p_parent_table;
		-- Stop data going into child tables and stop new id partitions from being made.
		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;
		v_trig_name := partman.check_name_length(p_object_name := v_parent_tablename, p_suffix := '_part_trig'); 
		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);

		SELECT tgname INTO v_trig_name FROM pg_catalog.pg_trigger t WHERE tgname = v_trig_name;
		IF v_trig_name IS NOT NULL THEN
		    -- lockwait for trigger drop
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                EXECUTE 'LOCK TABLE ONLY '||p_parent_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		            RAISE NOTICE 'Unable to obtain lock on parent table to remove trigger';
		            RETURN -1;
		        END IF;
		    END IF; -- END p_lock_wait IF
		    EXECUTE 'DROP TRIGGER IF EXISTS '||v_trig_name||' ON '||p_parent_table;
		END IF; -- END trigger IF
		v_lock_obtained := FALSE; -- reset for reuse later

		EXECUTE 'DROP FUNCTION IF EXISTS '||v_function_name||'()';

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM update_step(v_step_id, 'OK', 'Stopped partition creation process. Removed trigger & trigger function');
		END IF;

		<<outer_child_loop>>
		WHILE v_batch_loop_count < p_batch_count LOOP 
		    SELECT n.nspname||'.'||c.relname INTO v_child_table
		    FROM pg_inherits i 
		    JOIN pg_class c ON i.inhrelid = c.oid 
		    JOIN pg_namespace n ON c.relnamespace = n.oid 
		    WHERE i.inhparent::regclass = p_parent_table::regclass 
		    ORDER BY i.inhrelid ASC;

		    EXIT WHEN v_child_table IS NULL;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing child partition: '||v_child_table);
		    END IF;

		    EXECUTE 'SELECT min('||v_control||') FROM '||v_child_table INTO v_child_min;
		    IF v_child_min IS NULL THEN
		        -- No rows left in this child table. Remove from partition set.

		        -- lockwait timeout for table drop
		        IF p_lock_wait > 0  THEN
		            v_lock_iter := 0;
		            WHILE v_lock_iter <= 5 LOOP
		                v_lock_iter := v_lock_iter + 1;
		                BEGIN
		                    EXECUTE 'LOCK TABLE ONLY '||v_child_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                    v_lock_obtained := TRUE;
		                EXCEPTION
		                    WHEN lock_not_available THEN
		                        PERFORM pg_sleep( p_lock_wait / 5.0 );
		                        CONTINUE;
		                END;
		                EXIT WHEN v_lock_obtained;
		            END LOOP;
		            IF NOT v_lock_obtained THEN
		                RAISE NOTICE 'Unable to obtain lock on child table for removal from partition set';
		                RETURN -1;
		            END IF;
		        END IF; -- END p_lock_wait IF
		        v_lock_obtained := FALSE; -- reset for reuse later

		        EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		        IF p_keep_table = false THEN
		            EXECUTE 'DROP TABLE '||v_child_table;
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table DROPPED. Moved '||v_child_loop_total||' rows to parent');
		            END IF;
		        ELSE
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table UNINHERITED, not DROPPED. Moved '||v_child_loop_total||' rows to parent');
		            END IF;
		        END IF;
		        v_undo_count := v_undo_count + 1;
		        CONTINUE outer_child_loop;
		    END IF;
		    v_inner_loop_count := 1;
		    v_child_loop_total := 0;
		    <<inner_child_loop>>
		    LOOP
		        -- lockwait timeout for row batches
		        IF p_lock_wait > 0  THEN
		            v_lock_iter := 0;
		            WHILE v_lock_iter <= 5 LOOP
		                v_lock_iter := v_lock_iter + 1;
		                BEGIN
		                    EXECUTE 'SELECT * FROM ' || v_child_table ||
		                    ' WHERE '||v_control||' <= '||quote_literal(v_child_min + (p_batch_interval * v_inner_loop_count))
		                    ||' FOR UPDATE NOWAIT';
		                    v_lock_obtained := TRUE;
		                EXCEPTION
		                    WHEN lock_not_available THEN
		                        PERFORM pg_sleep( p_lock_wait / 5.0 );
		                        CONTINUE;
		                END;
		                EXIT WHEN v_lock_obtained;
		            END LOOP;
		            IF NOT v_lock_obtained THEN
		               RAISE NOTICE 'Unable to obtain lock on batch of rows to move';
		               RETURN -1;
		            END IF;
		        END IF;

		        -- Get everything from the current child minimum up to the multiples of the given interval
		        v_move_sql := 'WITH move_data AS (DELETE FROM '||v_child_table||
		                ' WHERE '||v_control||' <= '||quote_literal(v_child_min + (p_batch_interval * v_inner_loop_count))||' RETURNING *)
		            INSERT INTO '||p_parent_table||' SELECT * FROM move_data';
		        EXECUTE v_move_sql;
		        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		        v_total := v_total + v_rowcount;
		        v_child_loop_total := v_child_loop_total + v_rowcount;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Moved '||v_child_loop_total||' rows to parent.');
		        END IF;
		        EXIT inner_child_loop WHEN v_rowcount = 0; -- exit before loop incr if table is empty
		        v_inner_loop_count := v_inner_loop_count + 1;
		        v_batch_loop_count := v_batch_loop_count + 1;
		        EXIT outer_child_loop WHEN v_batch_loop_count >= p_batch_count; -- Exit outer FOR loop if p_batch_count is reached
		    END LOOP inner_child_loop;
		END LOOP outer_child_loop;

		IF v_batch_loop_count < p_batch_count THEN
		    -- FOR loop never ran, so there's no child tables left.
		    DELETE FROM partman.part_config WHERE parent_table = p_parent_table;
		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing config from pg_partman');
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;
		END IF;

		RAISE NOTICE 'Copied % row(s) to the parent. Removed % partitions.', v_total, v_undo_count;
		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Final stats');
		    PERFORM update_step(v_step_id, 'OK', 'Copied '||v_total||' row(s) to the parent. Removed '||v_undo_count||' partitions.');
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_total;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN UNDO PARTITIONING: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// undo_partition_time()
	_, err = tx.Exec(`
		/*
		 * Function to undo time-based partitioning created by this extension
		 */
		CREATE FUNCTION partman.undo_partition_time(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval interval DEFAULT NULL, p_keep_table boolean DEFAULT true, p_lock_wait numeric DEFAULT 0) RETURNS bigint
		    LANGUAGE plpgsql SECURITY DEFINER
		    AS $$
		DECLARE

		v_adv_lock              boolean;
		v_batch_loop_count      int := 0;
		v_child_min             timestamptz;
		v_child_loop_total      bigint := 0;
		v_child_table           text;
		v_control               text;
		v_function_name         text;
		v_inner_loop_count      int;
		v_lock_iter             int := 1;
		v_lock_obtained         boolean := FALSE;
		v_job_id                bigint;
		v_jobmon                boolean;
		v_jobmon_schema         text;
		v_move_sql              text;
		v_old_search_path       text;
		v_parent_schema         text;
		v_parent_tablename      text;
		v_part_interval         interval;
		v_row                   record;
		v_rowcount              bigint;
		v_step_id               bigint;
		v_sub_count             int;
		v_total                 bigint := 0;
		v_trig_name             text;
		v_undo_count            int := 0;

		BEGIN

		v_adv_lock := pg_try_advisory_xact_lock(hashtext('pg_partman undo_time_partition'));
		IF v_adv_lock = 'false' THEN
		    RAISE NOTICE 'undo_time_partition already running.';
		    RETURN 0;
		END IF;

		SELECT part_interval::interval
		    , control
		    , jobmon
		INTO v_part_interval
		    , v_control
		    , v_jobmon
		FROM partman.part_config 
		WHERE parent_table = p_parent_table 
		AND (type = 'time-static' OR type = 'time-dynamic' OR type = 'time-custom');

		IF v_part_interval IS NULL THEN
		    RAISE EXCEPTION 'Configuration for given parent table not found: %', p_parent_table;
		END IF;

		-- Check if any child tables are themselves partitioned or part of an inheritance tree. Prevent undo at this level if so.
		-- Need to either lock child tables at all levels or handle the proper removal of triggers on all child tables first 
		--  before multi-level undo can be performed safely.
		FOR v_row IN 
		    SELECT show_partitions AS child_table FROM partman.show_partitions(p_parent_table)
		LOOP
		    SELECT count(*) INTO v_sub_count 
		    FROM pg_catalog.pg_inherits
		    WHERE inhparent::regclass = v_row.child_table::regclass;
		    IF v_sub_count > 0 THEN
		        RAISE EXCEPTION 'Child table for this parent has child table(s) itself (%). Run undo partitioning on this table or remove inheritance first to ensure all data is properly moved to parent', v_row.child_table;
		    END IF;
		END LOOP;

		IF v_jobmon THEN
		    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
		    IF v_jobmon_schema IS NOT NULL THEN
		        SELECT current_setting('search_path') INTO v_old_search_path;
		        EXECUTE 'SELECT set_config(''search_path'',''partman,'||v_jobmon_schema||''',''false'')';
		    END IF;
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    v_job_id := add_job('PARTMAN UNDO PARTITIONING: '||p_parent_table);
		    v_step_id := add_step(v_job_id, 'Undoing partitioning for table '||p_parent_table);
		END IF;

		IF p_batch_interval IS NULL THEN
		    p_batch_interval := v_part_interval;
		END IF;

		-- Stops new time partitons from being made as well as stopping child tables from being dropped if they were configured with a retention period.
		UPDATE partman.part_config SET undo_in_progress = true WHERE parent_table = p_parent_table;
		-- Stop data going into child tables.
		SELECT schemaname, tablename INTO v_parent_schema, v_parent_tablename FROM pg_tables WHERE schemaname ||'.'|| tablename = p_parent_table;
		v_trig_name := partman.check_name_length(p_object_name := v_parent_tablename, p_suffix := '_part_trig'); 
		v_function_name := partman.check_name_length(v_parent_tablename, v_parent_schema, '_part_trig_func', FALSE);

		SELECT tgname INTO v_trig_name FROM pg_catalog.pg_trigger t WHERE tgname = v_trig_name;
		IF v_trig_name IS NOT NULL THEN
		    -- lockwait for trigger drop
		    IF p_lock_wait > 0  THEN
		        v_lock_iter := 0;
		        WHILE v_lock_iter <= 5 LOOP
		            v_lock_iter := v_lock_iter + 1;
		            BEGIN
		                EXECUTE 'LOCK TABLE ONLY '||p_parent_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                v_lock_obtained := TRUE;
		            EXCEPTION
		                WHEN lock_not_available THEN
		                    PERFORM pg_sleep( p_lock_wait / 5.0 );
		                    CONTINUE;
		            END;
		            EXIT WHEN v_lock_obtained;
		        END LOOP;
		        IF NOT v_lock_obtained THEN
		            RAISE NOTICE 'Unable to obtain lock on parent table to remove trigger';
		            RETURN -1;
		        END IF;
		    END IF; -- END p_lock_wait IF
		    EXECUTE 'DROP TRIGGER IF EXISTS '||v_trig_name||' ON '||p_parent_table;
		END IF; -- END trigger IF
		v_lock_obtained := FALSE; -- reset for reuse later

		EXECUTE 'DROP FUNCTION IF EXISTS '||v_function_name||'()';

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM update_step(v_step_id, 'OK', 'Stopped partition creation process. Removed trigger & trigger function');
		END IF;

		<<outer_child_loop>>
		WHILE v_batch_loop_count < p_batch_count LOOP 
		    SELECT n.nspname||'.'||c.relname INTO v_child_table
		    FROM pg_inherits i 
		    JOIN pg_class c ON i.inhrelid = c.oid 
		    JOIN pg_namespace n ON c.relnamespace = n.oid 
		    WHERE i.inhparent::regclass = p_parent_table::regclass 
		    ORDER BY i.inhrelid ASC;

		    EXIT WHEN v_child_table IS NULL;

		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing child partition: '||v_child_table);
		    END IF;

		    EXECUTE 'SELECT min('||v_control||') FROM '||v_child_table INTO v_child_min;
		    IF v_child_min IS NULL THEN
		        -- No rows left in this child table. Remove from partition set.

		        -- lockwait timeout for table drop
		        IF p_lock_wait > 0  THEN
		            v_lock_iter := 0;
		            WHILE v_lock_iter <= 5 LOOP
		                v_lock_iter := v_lock_iter + 1;
		                BEGIN
		                    EXECUTE 'LOCK TABLE ONLY '||v_child_table||' IN ACCESS EXCLUSIVE MODE NOWAIT';
		                    v_lock_obtained := TRUE;
		                EXCEPTION
		                    WHEN lock_not_available THEN
		                        PERFORM pg_sleep( p_lock_wait / 5.0 );
		                        CONTINUE;
		                END;
		                EXIT WHEN v_lock_obtained;
		            END LOOP;
		            IF NOT v_lock_obtained THEN
		                RAISE NOTICE 'Unable to obtain lock on child table for removal from partition set';
		                RETURN -1;
		            END IF;
		        END IF; -- END p_lock_wait IF
		        v_lock_obtained := FALSE; -- reset for reuse later

		        EXECUTE 'ALTER TABLE '||v_child_table||' NO INHERIT ' || p_parent_table;
		        IF p_keep_table = false THEN
		            EXECUTE 'DROP TABLE '||v_child_table;
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table DROPPED. Moved '||v_child_loop_total||' rows to parent');
		            END IF;
		        ELSE
		            IF v_jobmon_schema IS NOT NULL THEN
		                PERFORM update_step(v_step_id, 'OK', 'Child table UNINHERITED, not DROPPED. Moved '||v_child_loop_total||' rows to parent');
		            END IF;
		        END IF;
		        v_undo_count := v_undo_count + 1;
		        CONTINUE outer_child_loop;
		    END IF;
		    v_inner_loop_count := 1;
		    v_child_loop_total := 0;
		    <<inner_child_loop>>
		    LOOP
		        -- do some locking with timeout, if required
		        IF p_lock_wait > 0  THEN
		            v_lock_iter := 0;
		            WHILE v_lock_iter <= 5 LOOP
		                v_lock_iter := v_lock_iter + 1;
		                BEGIN
		                    EXECUTE 'SELECT * FROM ' || v_child_table ||
		                    ' WHERE '||v_control||' <= '||quote_literal(v_child_min + (p_batch_interval * v_inner_loop_count))
		                    ||' FOR UPDATE NOWAIT';
		                    v_lock_obtained := TRUE;
		                EXCEPTION
		                    WHEN lock_not_available THEN
		                        PERFORM pg_sleep( p_lock_wait / 5.0 );
		                        CONTINUE;
		                END;
		                EXIT WHEN v_lock_obtained;
		            END LOOP;
		            IF NOT v_lock_obtained THEN
		               RAISE NOTICE 'Unable to obtain lock on batch of rows to move';
		               RETURN -1;
		            END IF;
		        END IF;

		        -- Get everything from the current child minimum up to the multiples of the given interval
		        v_move_sql := 'WITH move_data AS (DELETE FROM '||v_child_table||
		                ' WHERE '||v_control||' <= '||quote_literal(v_child_min + (p_batch_interval * v_inner_loop_count))||' RETURNING *)
		            INSERT INTO '||p_parent_table||' SELECT * FROM move_data';
		        EXECUTE v_move_sql;
		        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		        v_total := v_total + v_rowcount;
		        v_child_loop_total := v_child_loop_total + v_rowcount;
		        IF v_jobmon_schema IS NOT NULL THEN
		            PERFORM update_step(v_step_id, 'OK', 'Moved '||v_child_loop_total||' rows to parent.');
		        END IF;
		        EXIT inner_child_loop WHEN v_rowcount = 0; -- exit before loop incr if table is empty
		        v_inner_loop_count := v_inner_loop_count + 1;
		        v_batch_loop_count := v_batch_loop_count + 1;
		        EXIT outer_child_loop WHEN v_batch_loop_count >= p_batch_count; -- Exit outer FOR loop if p_batch_count is reached
		    END LOOP inner_child_loop;
		END LOOP outer_child_loop;

		IF v_batch_loop_count < p_batch_count THEN
		    -- FOR loop never ran, so there's no child tables left.
		    DELETE FROM partman.part_config WHERE parent_table = p_parent_table;
		    IF v_jobmon_schema IS NOT NULL THEN
		        v_step_id := add_step(v_job_id, 'Removing config from pg_partman');
		        PERFORM update_step(v_step_id, 'OK', 'Done');
		    END IF;
		END IF;

		RAISE NOTICE 'Copied % row(s) to the parent. Removed % partitions.', v_total, v_undo_count;
		IF v_jobmon_schema IS NOT NULL THEN
		    v_step_id := add_step(v_job_id, 'Final stats');
		    PERFORM update_step(v_step_id, 'OK', 'Copied '||v_total||' row(s) to the parent. Removed '||v_undo_count||' partitions.');
		END IF;

		IF v_jobmon_schema IS NOT NULL THEN
		    PERFORM close_job(v_job_id);
		    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
		END IF;

		RETURN v_total;

		EXCEPTION
		    WHEN OTHERS THEN
		        IF v_jobmon_schema IS NOT NULL THEN
		            IF v_job_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''PARTMAN UNDO PARTITIONING: '||p_parent_table||''')' INTO v_job_id;
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
		            ELSIF v_step_id IS NULL THEN
		                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
		            END IF;
		            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
		            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
		        END IF;
		        RAISE EXCEPTION '%', SQLERRM;
		END
		$$;
	`)
	if err != nil {
		log.Error("%v", err)
	}

	tx.Commit()
}

// Sets up tables to keep track of partitions
func (db DB) loadSqlTables() {
	tx := db.MustBegin()

	var err error
	_, err = tx.Exec(`
		CREATE TABLE partman.part_config (
		    parent_table text NOT NULL,
		    control text NOT NULL,
		    type text NOT NULL,
		    part_interval text NOT NULL,
		    constraint_cols text[],
		    premake int NOT NULL DEFAULT 4,
		    inherit_fk boolean NOT NULL DEFAULT true,
		    retention text,
		    retention_schema text,
		    retention_keep_table boolean NOT NULL DEFAULT true,
		    retention_keep_index boolean NOT NULL DEFAULT true,
		    datetime_string text,
		    use_run_maintenance BOOLEAN NOT NULL DEFAULT true,
		    jobmon boolean NOT NULL DEFAULT true,
		    undo_in_progress boolean NOT NULL DEFAULT false,
		    CONSTRAINT part_config_parent_table_pkey PRIMARY KEY (parent_table),
		    CONSTRAINT positive_premake_check CHECK (premake > 0)
		);
		CREATE INDEX part_config_type_idx ON partman.part_config (type);
		-- this is apparently not something we need or will work: SELECT pg_catalog.pg_extension_config_dump('part_config', '');


		-- FK set deferrable because create_parent() inserts to this table before part_config
		CREATE TABLE partman.part_config_sub (
		    sub_parent text PRIMARY KEY REFERENCES partman.part_config (parent_table) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
		    , sub_type text NOT NULL
		    , sub_control text NOT NULL
		    , sub_part_interval text NOT NULL
		    , sub_constraint_cols text[]
		    , sub_premake int NOT NULL DEFAULT 4
		    , sub_inherit_fk boolean NOT NULL DEFAULT true
		    , sub_retention text
		    , sub_retention_schema text
		    , sub_retention_keep_table boolean NOT NULL DEFAULT true
		    , sub_retention_keep_index boolean NOT NULL DEFAULT true
		    , sub_use_run_maintenance BOOLEAN NOT NULL DEFAULT true
		    , sub_jobmon boolean NOT NULL DEFAULT true
		);

		-- Put constraint functions & definitions here because having them separate makes the ordering of their creation harder to control. Some require the above tables to exist first.

		/*
		 * Check function for config table partition types
		 */
		CREATE FUNCTION partman.check_partition_type (p_type text) RETURNS boolean
		    LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER
		    AS $$
		DECLARE
		v_result    boolean;
		BEGIN
		    SELECT p_type IN ('time-static', 'time-dynamic', 'time-custom', 'id-static', 'id-dynamic') INTO v_result;
		    RETURN v_result;
		END
		$$;

		ALTER TABLE partman.part_config
		ADD CONSTRAINT part_config_type_check 
		CHECK (partman.check_partition_type(type));

		/* 
		 * Ensure that sub-partitioned tables that are themselves sub-partitions have the same configuration options set when they are part of the same inheritance tree
		 */
		CREATE FUNCTION partman.check_subpart_sameconfig(text) RETURNS boolean
		    LANGUAGE sql STABLE
		    AS $$
		    WITH child_tables AS (
		        SELECT n.nspname||'.'||c.relname AS tablename
		        FROM pg_catalog.pg_inherits h
		        JOIN pg_catalog.pg_class c ON c.oid = h.inhrelid
		        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		        WHERE h.inhparent::regclass = $1::regclass
		    )
		    SELECT CASE 
		        WHEN count(*) <= 1 THEN
		            true
		        WHEN count(*) > 1 THEN
		           false
		       END
		    FROM (
		        SELECT DISTINCT sub_type
		            , sub_control
		            , sub_part_interval
		            , sub_constraint_cols
		            , sub_premake
		            , sub_inherit_fk
		            , sub_retention
		            , sub_retention_schema
		            , sub_retention_keep_table
		            , sub_retention_keep_index
		            , sub_use_run_maintenance
		            , sub_jobmon
		        FROM partman.part_config_sub a
		        JOIN child_tables b on a.sub_parent = b.tablename) x;
		$$;

		ALTER TABLE partman.part_config_sub
		ADD CONSTRAINT subpart_sameconfig_chk
		CHECK (partman.check_subpart_sameconfig(sub_parent));
	`)
	if err != nil {
		log.Error("%v", err)
	}

	// 92/tables/tables.sql
	_, err = tx.Exec(`
		CREATE TABLE partman.custom_time_partitions (
		    parent_table text NOT NULL
		    , child_table text NOT NULL
		    , partition_range tstzrange NOT NULL
		    , PRIMARY KEY (parent_table, child_table));
		CREATE INDEX custom_time_partitions_partition_range_idx ON partman.custom_time_partitions USING gist (partition_range);
	`)
	if err != nil {
		log.Error("%v", err)
	}

	tx.Commit()
}
