#!/usr/bin/env ruby

require 'colorize'
require 'elasticsearch'
require 'json'
require 'openssl'
require 'slop'

# 
# API
#
# https://rubydoc.info/gems/elasticsearch-api/Elasticsearch/API/Indices/Actions
#

opts = Slop::Options.new
opts.string '-n', '--name', 'Data set', required: true
opts.bool '-c', '--create', 'Create index'
opts.bool '-i', '--index', 'Load index'
opts.bool '-d', '--delete', 'Delete index'
opts.bool '-r', '--reindex', 'Reindex'
opts.bool '-s', '--status', 'Cluster status'

begin
  parsed = opts.parse ARGV
rescue => e
  puts "\nError: #{e.to_s}\n\n"
  puts opts
  exit
end

if ARGV.length < 2
  puts opts
  exit
end

index = parsed[:name]

client = Elasticsearch::Client.new(
  user: ENV['ELASTIC_USER'],
  password: ENV['ELASTIC_PASSWORD'],
  host: ENV['ELASTIC_HOST'],
  scheme: 'https',
  port: 9243)

if parsed[:delete] || parsed[:reindex]
  puts 'Deleting ...'
  client.indices.delete index: index
end

if parsed[:create] || parsed[:reindex]
  puts 'Creating ...'
  client.indices.create index: index,
    body: {
      settings: {
        number_of_shards: 1,
        number_of_replicas: 1,
        refresh_interval: "10s"
      },
      mappings: {
        dynamic: "true",
        properties: {
#           "holding.biblio.copyright": {
#             type: "date"
#           },
#           "holding.biblio.num_pages": {
#             type: "keyword"
#           }
        }
      }
    }
end

if parsed[:index] || parsed[:reindex]
  puts 'Importing ...'

  file = File.read("data/#{index}.json")
  data = JSON.parse(file)

  i = 0
  data.each_slice(100) do |group|
    i += 1
    batch_for_bulk = []
    group.each do |d|
      batch_for_bulk.push({ index: { _index: index } }.to_json)
      batch_for_bulk.push(d.to_json)
    end
    results = client.bulk(
      index: index,
      body: batch_for_bulk
    )
    puts "Batch #{i} of #{data.count/100} ... done"
    #puts JSON.pretty_generate(results)
    #exit
  end

end

if parsed[:status] || parsed[:reindex]
  print "\nGetting cluster status ... "
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  health = client.cluster.health
  finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  diff = finish - start # gets time is seconds as a float
  if health['status'] == 'green'
    puts 'green'.light_green
    puts "Took #{'%0.4f' % diff} ms"
  elsif health['status'] == 'yellow'
    puts 'yellow'.light_yellow
    puts "Took #{'%0.4f' % diff} ms"
  elsif health['status'] == 'red'
    puts 'red'.light_red
    puts "Took #{'%0.4f' % diff} ms"
  end
  puts
end

