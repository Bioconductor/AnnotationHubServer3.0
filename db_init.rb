require 'sequel'
require 'yaml'
require 'logger'

## This script expects these values to be set in config.yml:
## 'ahs_database_type'
## 'mysql_url'
## 'sqlite_filename'

unless defined? DB
    basedir = File.dirname(__FILE__)
    config = YAML.load_file("#{basedir}/config.yml")
    ENV['AHS_DATABASE_TYPE']=config['ahs_database_type']

    if `hostname` =~ /^ip-/ and ENV['AHS_DATABASE_TYPE'].nil?
        ENV['AHS_DATABASE_TYPE']='mysql'
    end

    mode = nil
    if ENV['AHS_DATABASE_TYPE'].nil? or 
      !(["mysql", "sqlite"].include? ENV['AHS_DATABASE_TYPE'])
        puts "environment variable AHS_DATABASE_TYPE must be set to"
        puts "mysql or sqlite."
        exit
    else
        mode = ENV['AHS_DATABASE_TYPE'].to_sym
    end

    url = nil
    if mode == :mysql
        url = config['mysql_url']
    else
        url = "sqlite://#{basedir}/#{config['sqlite_filename']}"
    end

    DB = Sequel.connect(url)
end

DB.loggers << Logger.new($stdout)
