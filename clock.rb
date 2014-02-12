require 'clockwork'
require 'librato/metrics'
require 'sequel'
require_relative 'pgstats'

user  = ENV['LIBRATO_USER']
token = ENV['LIBRATO_TOKEN']
raise 'missing LIBRATO_USER'  unless user
raise 'missing LIBRATO_TOKEN' unless token

Librato::Metrics.authenticate user, token

include Clockwork
counters = {}
every 5.minutes, 'postgres_performance' do
  uri = URI.parse(ENV["DATABASE_URL"])
  database_name = uri.path[1..-1]
  Sequel.connect(uri.to_s) do |db|
    PGStats.new(db: db, interval: 60 * 5, counters: counters, source: database_name).submit
  end
end
