#!/usr/bin/env ruby

require 'sinatra'
require 'sequel'
require 'date'
require 'pp'
require 'json'

mode = :mysql

url = nil
if mode == :mysql
    url = "mysql://ahuser:password@localhost/ahtest"
else
    url = "sqlite://#{File.dirname(__FILE__)}/ahtest.sqlite3"
end

DB = Sequel.connect(url)

require './models.rb'


def get_value(thing)
    return thing if thing.empty?
    thing.map{|i| i.values}
end

get "/newerthan/:date"  do
    # a date in the format 2014-04-01
    d = DateTime.strptime(params[:date], "%Y-%m-%d")
    x = Version.filter{rdatadateadded >  d}.select(:resource_id).all
    ids = x.map{|i| i.resource_id }
    r = Resource.filter(:id => ids).eager(:versions, :rdatapaths,
        :input_sources, :tags, :biocversions, :recipes).all
    out = []
    for row in r
        v = row.values
        v[:versions] = get_value row.versions
        v[:rdatapaths] = get_value row.rdatapaths
        v[:input_sources] = get_value row.input_sources
        v[:tags] = get_value row.tags
        v[:biocversions] = get_value row.biocversions
        v[:recipes] = get_value row.recipes
        out.push v
    end
    out.to_json
end

get "/schema_version" do
    if DB.table_exists? :schema_info
        DB[:schema_info].first[:version].to_s
    else
        "0"
    end
end