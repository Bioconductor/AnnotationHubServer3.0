#!/usr/bin/env ruby

require 'yaml'
require 'sequel'
require 'zlib'
require 'fileutils'
require 'socket'

ld0 = File::SEPARATOR + File.join("tmp", "ahlogs")
ld1 = File::SEPARATOR + File.join("var", "log", "apache2")

host = Socket.gethostname
if host =~ /dhcp|fhcrc/
    LOGDIR = ld0
elsif host =~ /^ip-/
    LOGDIR = ld1
else
    raise "Don't know where the logs are!"
end


DB0 = Sequel.sqlite("file://#{Dir.pwd}/annotationhub.sqlite3")

db1_path = File.join(Dir.pwd, "apachelogs.sqlite3")
FileUtils.rm db1_path if File.exist? db1_path

DB1 = Sequel.sqlite(db1_path)


DB1.create_table(:entries) do
    primary_key :id
    String :ip
    DateTime :timestamp
    String :source_url
    String :actual_url
end  

@rdatapaths = DB0[:rdatapaths]
@resources = DB0[:resources]
@location_prefixes = DB0[:location_prefixes]
@input_sources = DB0[:input_sources]
@entries = DB1[:entries]

logs = Dir.new(LOGDIR).entries.find_all{|i| i =~ /^access/}

def get_urls(num)
    rdp = @rdatapaths.where(:id => num).first
    return nil if rdp.nil?
    rsrc = @resources.where(:id => rdp[:resource_id]).first
    lp = @location_prefixes.where(:id => rsrc[:location_prefix_id]).first[:location_prefix]
    actual_url = lp + rdp[:rdatapath]
    rdps = @rdatapaths.where(:resource_id => rsrc[:id]).all
    idx = -1
    rdps.each_with_index do |item, i|
        if rdp[:id] == num
            idx = i
            break
        end
    end
    source_url = @input_sources.where(:resource_id => rsrc[:id]).all[idx][:sourceurl]
    {:source_url => source_url, :actual_url => actual_url}
end

def parse(line)
    #140.107.170.55 - - [26/Apr/2015:06:40:13 +0000] "GET /metadata/database_timestamp HTTP/1.1" 200 5734 "-" "curl/7.30.0 Rcurl/1.95.4.6 httr/0.6.1"
    return unless line.index("/fetch/")
    regex = /^(\S+) \S+ \S+ \[([^\]]+)\] "[A-Z]+ ([^"]*)" \d+ \d+ "[^"]*" "([^"]*)"$/m
    m = regex.match line
    path = m.captures[2]
    return unless path.start_with? "/fetch/"
    num = path.split(' ').first.split('/').last.to_i
    return if num == 0
    h = get_urls(num)
    return if h.nil?
    timestamp = DateTime.strptime(m.captures[1], "%d/%b/%Y:%H:%M:%S %z")
    @entries.insert({:ip => m.captures.first, :timestamp => timestamp,
        :source_url => h[:source_url], :actual_url => h[:actual_url]})
end

for log in logs
    logpath = File.join(LOGDIR, log)
    f = File.open(logpath) do |f|
        if logpath.end_with? ".gz"
            gz = Zlib::GzipReader.new(f)
            gz.each_line {|i| parse i.strip}
            gz.close
        else
            lines = f.readlines
            lines.each {|i| parse i.strip}
        end
    end
end

