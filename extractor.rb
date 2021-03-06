#!/usr/bin/env ruby

require 'colorize'
require 'json'
require 'nokogiri'
require 'securerandom'
require 'slop'
require 'tzinfo'

opts = Slop::Options.new
opts.string '-f', '--file', 'Apollo export file', required: true

begin
  parsed = opts.parse ARGV
rescue => e
  puts "\nError: #{e.to_s}\n\n"
  puts opts
  exit
end

print "Reading #{parsed[:file]} ... "
starting = Time.now

doc = File.open(parsed[:file]) { |f| Nokogiri::XML(f) }
puts doc.errors
puts "done\n".light_green

memberships = {}
doc.xpath('//patronMembership').each do |m|
  memberships[m['id']] = m['name'].split.each { |x| x.capitalize! }.join(' ')
end

categories = {}
doc.xpath('//holdingMembership').each do |m|
  categories[m['id']] = m['name']
end

#
# Datetimes are stored in the libraries local time (and they're not in ISO 8601 format).
# We'll need to convert any datetimes to UTC and rewrite them in ISO 8601 format.
#
# Specify the timezone of the library's local time here:
#
tz = TZInfo::Timezone.get('America/New_York')

#
# Biblios
#
print "Extracting //biblio to data/biblios.json ... "
biblios = {}
doc.xpath('//biblio').each do |p|
  biblio = {}

  if biblios.key?(p['id'])
    puts "#{p['id']} already seen"
    exit
  else
    biblio['id'] = 'B' + SecureRandom.hex(4) 
  end
  if p['added']
    t = DateTime.iso8601(p['added'])
    biblio['added'] = tz.local_to_utc(t).iso8601
  end
  if p['edited']
    t = DateTime.iso8601(p['edited'])
    biblio['edited'] = tz.local_to_utc(t).iso8601
  end
  if p['deleted']
    t = DateTime.iso8601(p['deleted'])
    biblio['deleted'] = tz.local_to_utc(t).iso8601
  end
  biblio['usage_count'] = p['usageCount'].to_i
  biblio['status'] = p['status']

  p.xpath('marc:record/marc:controlfield').each do |cf|
    biblio['control_number'] = cf.content if cf['tag'] == '001'
    biblio['control_number_identifier'] = cf.content if cf['tag'] == '003'
  end

  p.xpath('marc:record/marc:datafield').each do |df|
    if df['tag'] == '020'
      df.xpath('marc:subfield').each do |sf|
        biblio['isbn'] = sf.content if sf['code'] == 'a'
        if sf['code'] == 'c'
          biblio['price'] = sf.content.gsub(/\$/, "").to_f
        end
      end
    elsif df['tag'] == '040'
      df.xpath('marc:subfield').each do |sf|
        biblio['cataloging_source'] = sf.content if sf['code'] == 'd'
      end
    elsif df['tag'] =~ /^1/
      df.xpath('marc:subfield').each do |sf|
        if biblio['author'] == nil
          biblio['author'] = sf.content
        else
          biblio['author'] += " " + sf.content
        end
      end
    elsif df['tag'] == '245'
      df.xpath('marc:subfield').each do |sf|
        if biblio['title'] == nil
          biblio['title'] = sf.content
        else
          biblio['title'] += " " + sf.content
        end
      end
    elsif df['tag'] == '264'
      df.xpath('marc:subfield').each do |sf|
        biblio['publication'] = sf.content if sf['code'] == 'a'
        biblio['copyright'] = sf.content.gsub(/[^0-9]/, '')[0..3].to_i if sf['code'] == 'c'
      end
    elsif df['tag'] == '300'
      df.xpath('marc:subfield').each do |sf|
        if sf['code'] == 'a'
          pages = sf.content.gsub(/[^0-9]/, '').to_i
          biblio['num_pages'] = pages if pages > 0 && pages < 1000
        end
      end
    end
  end

  if biblio['title']
    biblio['title'] = biblio['title'].chomp('.')
  else
    biblio['title'] = 'No Title'
  end
  biblios[p['id']] = biblio
end

File.open("data/biblios.json", 'w') do |file|
  file.write(biblios.values.to_json)
  puts "done".light_green
end

#
# Patrons, Addresses, Fines, Reserves
# Since there may be multiple <addresses>, <fines>, and <reserves> per <patron>,
# and each is more than one field, they should get their own index in Elasticsearch.
#
patrons = {}
addresses = []
fines = []
reserves = []
doc.xpath('//patron').each do |p|
  patron = {}

  if patrons.key?(p['id'])
    puts "#{p['id']} already seen"
    exit
  else
    patron['id'] = 'P' + SecureRandom.hex(4) 
  end

  patron['usage_count'] = p['usageCount']
  if p['edited']
    t = DateTime.iso8601(p['edited'])
    patron['edited'] = tz.local_to_utc(t).iso8601
  end
  if p['expiration']
    t = DateTime.iso8601(p['expiration'])
    patron['expiration'] = tz.local_to_utc(t).iso8601
  end
  if p['latest_activity']
    t = DateTime.iso8601(p['latest_activity'])
    patron['latest_activity'] = tz.local_to_utc(t).iso8601
  end
  if p['created']
    t = DateTime.iso8601(p['created'])
    patron['created'] = tz.local_to_utc(t).iso8601
  end
  p.xpath('membership').each do |mb|
    patron['membership'] = memberships[mb.content] 
  end
  address_count = 0
  p.xpath('addresses/address').each do |al|
    address_count += 1
    address = {}
    address['mailing'] = al['mailing']
    address['state'] = al['countryDivision']
    address['county'] = al['locality']
    address['postal'] = al['postalCode']
    address['country'] = al['country']
    address['patron'] = patron['id']
    address['membership'] = patron['membership']
    address['timestamp'] = patron['created']
    addresses << address
  end
  patron['address_count'] = address_count
  phones = []
  p.xpath('phones/phone').each do |ph|
    phones << ph['type']
  end
  patron['phones'] = phones
  patron['phones_count'] = phones.length
  fines_count = 0
  p.xpath('fines/fine').each do |fn|
    fines_count += 1
    fine = {}
    fine['continuation'] = fn['continuation']
    fine['amount_cents'] = fn['amountCents']
    fine['status'] = fn['status']
    fine['amount_paid_cents'] = fn['amountPaidCents']
    if fn['returned']
      t = DateTime.iso8601(fn['returned'])
      fine['returned'] = tz.local_to_utc(t).iso8601
    end
    fine['patron'] = patron['id']
    fine['membership'] = patron['membership']
    fines << fine
  end
  patron['fines_count'] = fines_count
  reserves_count = 0
  p.xpath('reserves/reserve').each do |rs|
    reserves_count += 1
    reserve = {}
    reserve['status'] = rs['status']
    if rs['placed']
      t = DateTime.iso8601(rs['placed'])
      reserve['placed'] = tz.local_to_utc(t).iso8601
    end
    if rs['resolved']
      t = DateTime.iso8601(rs['resolved'])
      reserve['resolved'] = tz.local_to_utc(t).iso8601
    end
    reserve['biblio'] = biblios[rs['biblio']]['id']
    reserve['patron'] = patron['id']
    reserve['membership'] = patron['membership']
    reserves << reserve
  end
  patron['reserves_count'] = reserves_count
  patrons[p['id']] = patron
end

File.open("data/patrons.json", 'w') do |file|
  print "Extracting //patron to data/patrons.json ... "
  file.write(patrons.values.to_json)
  puts "done".light_green
end

File.open("data/addresses.json", 'w') do |file|
  print "Extracting //address to data/addresses.json ... "
  file.write(addresses.to_json)
  puts "done".light_green
end

File.open("data/fines.json", 'w') do |file|
  print "Extracting //fine to data/fines.json ... "
  file.write(fines.to_json)
  puts "done".light_green
end

File.open("data/reserves.json", 'w') do |file|
  print "Extracting //reserve to data/reserves.json ... "
  file.write(reserves.to_json)
  puts "done".light_green
end

#
# Holdings
#
print "Extracting //holding to data/holdings.json ... "
holdings = {}
doc.xpath('//holding').each do |p|
  holding = {}

  if holdings.key?(p['id'])
    puts "#{p['id']} already seen"
    exit
  else
    holding['id'] = 'H' + SecureRandom.hex(4) 
  end

  holding['deleted_type'] = p['deletedType']
  holding['usage_count'] = p['usageCount'].to_i
  holding['status'] = p['status']
  holding['call'] = p['call']
  if p['added']
    t = DateTime.iso8601(p['added'])
    holding['added'] = tz.local_to_utc(t).iso8601
  end
  if p['edited']
    t = DateTime.iso8601(p['edited'])
    holding['edited'] = tz.local_to_utc(t).iso8601
  end
  if p['deleted']
    t = DateTime.iso8601(p['deleted'])
    holding['deleted'] = tz.local_to_utc(t).iso8601
  end
  holding['barcode'] = p['barcode']
  holding['price_list_cents'] = p['priceListCents'].to_i
  holding['price_cents'] = p['priceCents'].to_i
  holding['membership'] = []
  holding['category'] = []
  holding['is_dvd'] = false
  p.xpath('membership').each do |m|
    holding['membership'] << memberships[m.content] if m.content =~ /^pm/
    if m.content =~ /^hm/ && m.content != 'hm51'
      if m.content == 'hm50'
        holding['is_dvd'] = true
      else
        holding['category'] << categories[m.content]
      end
    end
  end
  if biblios.key?(p['biblio'])
    holding['biblio'] = biblios[p['biblio']]
  else
    puts "Holding #{p['id']} points to Biblio #{p['biblio']} with no match"
    exit
  end
  holdings[p['id']] = holding
end

File.open("data/holdings.json", 'w') do |file|
  file.write(holdings.values.to_json)
  puts "done".light_green
end

#
# Checkouts
#
print "Extracting //checkout to data/checkouts.json ... "
checkouts = {}
doc.xpath('//checkout').each do |d|
  checkout = {}
  checkout['type'] = d['type']
  # We need to test to see if we're converting the local time to UTC and obeying DST.
  #next unless d['out'] == "2020-01-14T09:48:05"
  #next unless d['out'] == "2020-06-29T19:32:27"
  #puts "Time #{d['out']} is: #{t}"
  #puts "Time #{d['out']} is: #{tz.local_to_utc(time)}"
  #puts "Time #{d['out']} is: #{tz.strftime('%Y-%m-%d %H:%M:%S %Z', tz.local_to_utc(time))}"
  #puts "Time #{d['out']} is: #{tz.local_to_utc(time).iso8601}"
  # Test EST Conversion
  # Time 2020-01-14T09:48:05 is: 2020-01-14T09:48:05+00:00
  # Time 2020-01-14T09:48:05 is: 2020-01-14 14:48:05 UTC
  # Time 2020-01-14T09:48:05 is: 2020-01-14 09:48:05 EST <<<<<< It detects EST
  # Time 2020-01-14T09:48:05 is: 2020-01-14T14:48:05Z
  # Test EDT Conversion
  # Time 2020-06-29T19:32:27 is: 2020-06-29T19:32:27+00:00
  # Time 2020-06-29T19:32:27 is: 2020-06-29 23:32:27 UTC
  # Time 2020-06-29T19:32:27 is: 2020-06-29 19:32:27 EDT <<<<<< It detects EDT
  # Time 2020-06-29T19:32:27 is: 2020-06-29T23:32:27Z
  if d['out']
    t = DateTime.iso8601(d['out'])
    checkout['out'] = tz.local_to_utc(t).iso8601
    checkout['out_day_of_week'] = Date.iso8601(d['out']).strftime("%A") if d['out']
  else
    puts "Checkout #{d['id']} has no out"
    exit
  end
  if d['due']
    t = DateTime.iso8601(d['due'])
    checkout['due'] = tz.local_to_utc(t).iso8601
    checkout['due_day_of_week'] = Date.iso8601(checkout['due']).strftime("%A")
  end
  if d['due']
    t = DateTime.iso8601(d['due'])
    checkout['due'] = tz.local_to_utc(t).iso8601
    checkout['due_day_of_week'] = Date.iso8601(checkout['due']).strftime("%A")
  end
  checkout['status'] = d['status']
  if d['returned']
    t = DateTime.iso8601(d['returned'])
    checkout['returned'] = tz.local_to_utc(t).iso8601
  end
  if patrons.key?(d['patron'])
    checkout['patron'] = patrons[d['patron']]
  else
    # Many checkouts do not link to a Patron
    # puts "Checkout #{d['id']} has no Patron #{d['patron']} match"
  end
  if holdings.key?(d['holding'])
    checkout['holding'] = holdings[d['holding']]
  else
    # Many checkouts do not link to a Holding
    # puts "Checkout #{d['id']} has no Holding #{d['holding']} match"
  end
  checkout['reserved'] = true if d['reserveId']
  checkout['renewals_count'] = d.xpath('renewals/renewal').count
  checkout['membership'] = []
  checkout['category'] = []
  # Patron memberships are recorded in each checkout and in each Patron.
  # Holding memberships are recorded in each checkout and in each Holding.
  # We'll extract both of them but their could be discrepancies between sources.
  checkout['is_dvd'] = false
  d.xpath('membership').each do |m|
    checkout['membership'] << memberships[m.content] if m.content =~ /^pm/
    if m.content =~ /^hm/ && m.content != 'hm51'
      if m.content == 'hm50'
        checkout['is_dvd'] = true
      else
        checkout['category'] << categories[m.content]
      end
    end
  end
  checkouts[d['id']] = checkout
end

File.open("data/checkouts.json", 'w') do |file|
  file.write(checkouts.values.to_json)
  puts "done\n".light_green
end

ending = Time.now
elapsed = ending - starting
if elapsed > 60
  puts "Completed in #{(elapsed / 60).round(0)} min #{(elapsed % 60).round(0)} sec"
else
  puts "Completed in #{elapsed.round(0)} sec"
end
