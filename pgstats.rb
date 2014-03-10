require 'librato/metrics'

class PGStats
  def initialize(options = {})
    @db       = options.fetch(:db)
    @counters = options.fetch(:counters) { Hash.new }
    @interval = options.fetch(:interval) { 60 }
    @client   = options.fetch(:client) { Librato::Metrics.client }
    @source   = options.fetch(:source) { 'pg-stats-aggregator' }
  end

  def stats
    @db[<<-SQL].all
      SELECT sum(seq_scan)  AS sequence_scans,
             sum(idx_scan)  AS index_scans,
             sum(n_tup_ins) AS inserts,
             sum(n_tup_upd) AS updates,
             sum(n_tup_del) AS deletes
      FROM pg_stat_user_tables;
    SQL
  end

  def index_hit_rate
    @db[<<-SQL].all
      SELECT sum(idx_blks_hit) / sum(idx_blks_hit + idx_blks_read) AS index_hit_rate
      FROM pg_statio_user_indexes;
    SQL
  end

  def cache_hit_rate
    @db[<<-SQL].all
      SELECT sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS cache_hit_rate
      FROM pg_statio_user_tables;
    SQL
  end

  def index_size
    @db[<<-SQL].all
      SELECT sum(relpages::bigint*8192)::bigint AS total_index_size
      FROM pg_class 
      WHERE reltype = 0; 
    SQL
  end

  def unused_index
    @db[<<-SQL].all
    SELECT 
        schemaname || '.' || relname AS table, 
        indexrelname AS index, 
        pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size, 
        idx_scan as index_scans 
      FROM pg_stat_user_indexes ui 
      JOIN pg_index i ON ui.indexrelid = i.indexrelid 
      WHERE NOT indisunique AND idx_scan < 50 AND pg_relation_size(relid) > 5 * 8192 
      ORDER BY pg_relation_size(i.indexrelid) / nullif(idx_scan, 0) DESC NULLS FIRST, 
      pg_relation_size(i.indexrelid) DESC; 
    SQL
  end

  def long_running_queries
    @db[<<-SQL].all
      SELECT
        pid,
        now() - pg_stat_activity.query_start AS duration,
        query AS query
      FROM
        pg_stat_activity
      WHERE
        pg_stat_activity.query <> ''::text
        AND state <> 'idle'
        AND now() - pg_stat_activity.query_start > interval '5 minutes'
      ORDER BY
        now() - pg_stat_activity.query_start DESC;
    SQL
  end

  def bloat
    @db[<<-SQL].all
        WITH constants AS (
          SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
        ), bloat_info AS (
          SELECT
            ma,bs,schemaname,tablename,
            (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
          FROM (
            SELECT
              schemaname, tablename, hdr, ma, bs,
              SUM((1-null_frac)*avg_width) AS datawidth,
              MAX(null_frac) AS maxfracsum,
              hdr+(
                SELECT 1+count(*)/8
                FROM pg_stats s2
                WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
            FROM pg_stats s, constants
            GROUP BY 1,2,3,4,5
          ) AS foo
        ), table_bloat AS (
          SELECT
            schemaname, tablename, cc.relpages, bs,
            CEIL((cc.reltuples*((datahdr+ma-
              (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
          FROM bloat_info
          JOIN pg_class cc ON cc.relname = bloat_info.tablename
          JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
        ), index_bloat AS (
          SELECT
            schemaname, tablename, bs,
            COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
            COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
          FROM bloat_info
          JOIN pg_class cc ON cc.relname = bloat_info.tablename
          JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
          JOIN pg_index i ON indrelid = cc.oid
          JOIN pg_class c2 ON c2.oid = i.indexrelid
        )
        SELECT
          type, schemaname, object_name, bloat, pg_size_pretty(raw_waste) as waste
        FROM
        (SELECT
          'table' as type,
          schemaname,
          tablename as object_name,
          ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
          CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
        FROM
          table_bloat
            UNION
        SELECT
          'index' as type,
          schemaname,
          tablename || '::' || iname as object_name,
          ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
          CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
        FROM
          index_bloat) bloat_summary
        ORDER BY raw_waste DESC, bloat DESC;
    SQL
  end

  def vacuum_stats
    @db[<<-SQL].all
      WITH table_opts AS (
        SELECT
          pg_class.oid, relname, nspname, array_to_string(reloptions, '') AS relopts
        FROM
           pg_class INNER JOIN pg_namespace ns ON relnamespace = ns.oid
      ), vacuum_settings AS (
        SELECT
          oid, relname, nspname,
          CASE
            WHEN relopts LIKE '%autovacuum_vacuum_threshold%'
              THEN regexp_replace(relopts, '.*autovacuum_vacuum_threshold=([0-9.]+).*', E'\\\\\\1')::integer
              ELSE current_setting('autovacuum_vacuum_threshold')::integer
            END AS autovacuum_vacuum_threshold,
          CASE
            WHEN relopts LIKE '%autovacuum_vacuum_scale_factor%'
              THEN regexp_replace(relopts, '.*autovacuum_vacuum_scale_factor=([0-9.]+).*', E'\\\\\\1')::real
              ELSE current_setting('autovacuum_vacuum_scale_factor')::real
            END AS autovacuum_vacuum_scale_factor
        FROM
          table_opts
      )
      SELECT
        vacuum_settings.nspname AS schema,
        vacuum_settings.relname AS table,
        to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') AS last_vacuum,
        to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
        to_char(pg_class.reltuples, '9G999G999G999') AS rowcount,
        to_char(psut.n_dead_tup, '9G999G999G999') AS dead_rowcount,
        to_char(autovacuum_vacuum_threshold
             + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples), '9G999G999G999') AS autovacuum_threshold,
        CASE
          WHEN autovacuum_vacuum_threshold + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples) < psut.n_dead_tup
          THEN 'yes'
        END AS expect_autovacuum
      FROM
        pg_stat_user_tables psut INNER JOIN pg_class ON psut.relid = pg_class.oid
          INNER JOIN vacuum_settings ON pg_class.oid = vacuum_settings.oid
      ORDER BY 1;
    SQL
  end

  def blocking_queries
    @db[<<-SQL].all
      SELECT bl.pid     AS blocked_pid,
             a.usename  AS blocked_user,
             kl.pid     AS blocking_pid,
             ka.usename AS blocking_user,
             a.query    AS blocked_statement
       FROM  pg_catalog.pg_locks         bl
        JOIN pg_catalog.pg_stat_activity a  ON a.pid = bl.pid
        JOIN pg_catalog.pg_locks         kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid
        JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
       WHERE NOT bl.granted; 
    SQL
  end

  def table_locks
    @db[<<-SQL].all
     SELECT 
       pg_stat_activity.pid, 
       pg_class.relname, 
       pg_locks.transactionid, 
       pg_locks.granted, 
       substr(pg_stat_activity.query,1,30) AS query_snippet, 
       age(now(),pg_stat_activity.query_start) AS "age" 
     FROM pg_stat_activity,pg_locks left 
     OUTER JOIN pg_class 
       ON (pg_locks.relation = pg_class.oid) 
     WHERE pg_stat_activity.query <> '<insufficient privilege>' 
       AND pg_locks.pid = pg_stat_activity.pid
       AND pg_locks.mode = 'ExclusiveLock' 
       AND pg_stat_activity.pid <> pg_backend_pid() order by query_start; 
    SQL
  end

  # Returns a list of results like {:relname=>"zip_state", :percent_of_times_index_used=>"0", :rows_in_table=>0}
  def tables_index_usage
    @db[<<-SQL].all
      SELECT 
         relname, 
         CASE 
           WHEN idx_scan > 0 THEN (100 * idx_scan / (seq_scan + idx_scan))::text 
           ELSE '0'
         END percent_of_times_index_used, 
         n_live_tup rows_in_table
       FROM 
         pg_stat_user_tables 
       WHERE schemaname = 'public'
       ORDER BY 
         n_live_tup DESC; 
    SQL
  end

  def to_queuable(options = {})
    name = options.fetch(:name)
    value = options.fetch(:value)
    measure_time = options.fetch(:measure_time)
    source = options.fetch(:source) { @source }

    { "postgres.#{name}" => { value: value, measure_time: measure_time, source: source } }
  end

  def submit
    queue        = @client.new_queue
    measure_time = now_floored

    stats.first.each do |name, current_counter|
      current_counter = current_counter.to_i
      last_counter    = @counters[name]
      if last_counter && current_counter >= last_counter
        value = current_counter - last_counter
        queue.add(to_queuable(name: name, value: value, measure_time: measure_time))
      end

      @counters[name] = current_counter
    end

    index_hit_rate.first.each do |name, value|
      queue.add(to_queuable(name: name, value: value, measure_time: measure_time))
    end

    cache_hit_rate.first.each do |name, value|
      queue.add(to_queuable(name: name, value: value, measure_time: measure_time))
    end

    index_size.first.each do |name, value|
      queue.add(to_queuable(name: name, value: value, measure_time: measure_time))
    end

    tables_index_usage.each do |result|
      queue.add(to_queuable(name: "#{result[:relname]}.percent_of_times_index_used", value: result[:percent_of_times_index_used], measure_time: measure_time))
    end 

    queue.submit unless queue.empty?
  end

  def now_floored
    time = Time.now.to_i
    time - (time % @interval)
  end
end
