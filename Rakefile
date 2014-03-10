#!/usr/bin/env rake
require 'sequel'
require 'hirb'
require 'uri'
require_relative 'pgstats'

def pgstats_to_console(method_name)
  Sequel.connect(database_url) do |db| 
    render(PGStats.new(db: db).send(method_name.to_sym))
  end
end

def render(table)
  puts Hirb::Helpers::Table.render(table, :resize => true, :max_width => 200)
end

def database_url
  database_identifier = ARGV.last || ''
  task database_identifier.to_sym do ; end
  url = ENV[database_identifier] || ''
  raise "Missing database url for #{database_identifier}. Specify a config variable name after your command to select a database url." if url.to_s.empty?
  url
end

desc "Prints psql for connecting to database."
task :psql do 
  uri = URI.parse(database_url)
  p "PGPASSWORD='#{uri.password}' psql -U #{uri.user} -h #{uri.host} -d #{uri.path[1..-1]}"
end

desc "Counts scans, inserts, updates, and deletes."
task :stats do
  pgstats_to_console(:stats)
end

desc "Percentage of time the database uses an index, should be +99%."
task :index_hit_rate do
  pgstats_to_console(:index_hit_rate)
end

desc "Percentage of time the database uses the cache, should be +99%."
task :cache_hit_rate do
  pgstats_to_console(:cache_hit_rate)
end

desc "Total size of the index in bytes, divide by x/1048576 for MB."
task :index_size do
  pgstats_to_console(:index_size)
end

desc "Percentage of time index is used for a table and shows number of rows in table."
task :tables_index_usage do 
  pgstats_to_console(:tables_index_usage)
end

desc "Shows table, index name, and index size of unused indexes."
task :unused_index do 
  pgstats_to_console(:unused_index)
end

desc "Shows queries running for >5m, if any."
task :long_running_queries do 
  pgstats_to_console(:long_running_queries)
end

desc "Shows table bloat; tables with >10 bloat and >100MB waste should be vacuumed."
task :bloat do 
  pgstats_to_console(:bloat)
end

desc "Shows how often a table is vacuumed and when the next vacuum is expected."
task :vacuum_stats do 
  pgstats_to_console(:vacuum_stats)
end

desc "Shows blocking queries, if any."
task :blocking_queries do
  pgstats_to_console(:blocking_queries)
end

desc "Shows table locks, if any."
task :table_locks do
  pgstats_to_console(:table_locks)
end
