require 'clockwork'
require 'librato/metrics'
require 'sequel'
require_relative 'pgstats'

user  = ENV['LIBRATO_USER']
token = ENV['LIBRATO_TOKEN']
raise 'missing LIBRATO_USER'  unless user
raise 'missing LIBRATO_TOKEN' unless token

COLLECTION_INTERVAL = 5
Librato::Metrics.authenticate user, token

include Clockwork
counters = {}
every COLLECTION_INTERVAL.minutes, 'postgres_performance' do
  database_urls = ENV.keys.select{|k| k =~ /DATABASE_URL/}.map{|k| ENV[k]}
  uris = database_urls.map{|url| URI.parse(url) }
  uris.each do |uri|
    database_name = uri.path[1..-1]
    Sequel.connect(uri.to_s) do |db|
      PGStats.new(db: db, interval: 60 * COLLECTION_INTERVAL, counters: counters, source: database_name).submit
    end
  end
end
