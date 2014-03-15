#!/usr/bin/env ruby
require 'active_record'
require 'aws-sdk'
class CreatePimAdImpressions < ActiveRecord::Migration
  def self.up
    drop_table :pim_ad_impressions if ActiveRecord::Base.connection.table_exists? 'pim_ad_impressions'
    create_table :pim_ad_impressions do |t|
      t.integer :pim_id
      t.integer :content_element_id
      t.datetime :played_at
      t.integer :volume
      t.timestamps
    end
  end
end
  
class Batch < Array

  def self.transaction
    import = new
    begin
      yield import
    rescue =>e
      import.rollback
    end
    import
  end
  def initialize(*args)
    super(*args)
    @errored = []
    @total = []
  end
  def push(*args)
    @total.push(*args)
    super(*args)
  end
  def error(item, errors)
    @total.push(item)
    @errored.push(item,errors)
  end
  def errors
    @errored
  end
  def total
    @total
  end
  def worked
    self
  end
  def rollback
    each do |i|
      next if i.destroyed? || i.new_record?
      i.destroy
    end
    clear
  end
end

class PimAdImpressions < ActiveRecord::Base
  def self.store_impressions(impressions)
    worked = []
    errored = []
    ActiveRecord::Base.transaction do
      impressions.each do |i|
        i['played_at'] = Time.parse(i['played_at']).utc
        imp = self.create(i)
        puts imp.inspect
        #we want to send the row back and the errors
        if imp.errors.present?
          errored.push [i.to_json, imp.errors]
        else
          worked.push imp
        end 
      end
    end
    
    return [worked,errored] 
  # rescue => e # TODO: error handling
  #   raise e # until error handling
  end
end

class RawImpressions
  attr_reader :bucket
  def initialize(bucket_name)
    s3 = AWS::S3.new
    bucket = s3.buckets[bucket_name]
    @bucket = bucket
  end
  def create(pim_id,file, now=Time.now.utc)
    raise ArgumentError.new("Not a pim_id: #{pim_id}") unless Integer(pim_id)
    raise ArgumentError.new("No file body supplied: #{file}") unless file && file.respond_to?(:read)
    raise ArgumentError.new("File body is not JSON") unless JSON.parse(file.read) && file.rewind
    name = ["raw-impressions",pim_id,now.strftime("%Y-%m-%d/%H:%m%s.json")].join('/')
    bucket.objects[name].write(file)
  end
  def each
    bucket.objects.with_prefix('raw-impressions/').each do |i|
      next if i.key[-1] == "/"
      yield self,i
    end
  end
  def archive(i)
    new_name = i.key.sub('raw-impressions/','archived-impressions/')
    i.move_to(new_name)
  end
  def errchive(i, data)
    new_name = i.key.sub('raw-impressions/','impressions-errors/')
    bucket.objects.create("#{new_name}.err",data)
  end
    
  include Enumerable
end
class ImportRunner
def process_raw_impressions(bucket,i)
  #store a metadata file that includes # of rows that should have been entered into the db?
  ActiveRecord::Base.transaction do
    items = JSON.parse(i.read)['collection']['items']
    total_to_process = items.count
    worked, errored = PimAdImpressions.store_impressions(items)
    if worked.count == items.count
      bucket.archive(i)
      log_impression_processing(i, worked,errored)
    else
      #if a particular file fails, we want to log the errors
      #we probably want to move the file to a different dir, but let's leave it here now
      log_impression_processing_failed(i,errored)
      new_name = i.key.sub('raw-impressions/','impressions-errors/')
        bucket.errchive(i,<<-END
worked: #{import.worked.count}
errored: #{import.errored.count}
error data: #{import.errored.to_yaml}
END
                                     )
    end
  end
#rescue => e
  #@errors.push([i,e])
#  log_impression_processing_failed(i,errored)
#else
#  log_impression_processing(i, worked,errored)
end

def log_impressions_starting
  puts "raw_impressions_key,inserted,failed"
end

def log_impression_processing(i, worked,errored)
  puts "#{i.key.inspect},#{worked.length},#{errored.length}"
end

def log_impression_processing_failed(i,errored)
  puts "#{i.key.inspect},failed,#{errored.length}"
end

def import_all_impressions(bucket_name)
  RawImpressions.new(bucket_name).each do|bucket, i|
    process_raw_impressions(bucket,i)
  end
end


  def run?
    File.expand_path($0) == File.expand_path(__FILE__)
  end

  def ar_config
     YAML::load(ERB.new(File.read(File.expand_path('../../config/database.yml',__FILE__))).result(binding))
  end
  def connect_aws
     
    required_env = %w(AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_S3_BUCKET)

    fail("Please supply #{required_env.join(', ')}") unless required_env.all?{|i|ENV[i]}

    AWS.config(
           :access_key_id => ENV['AWS_ACCESS_KEY_ID'],
           :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'])
  end
  def connect_ar
	require 'erb'
	ActiveRecord::Base.configurations= ar_config
	ActiveRecord::Base.establish_connection(ActiveRecord::Base.configurations[deploy_env])
  end
  def deploy_env
    ENV['RAILS_ENV'] || 'development'
  end
  def connect
    connect_aws
    connect_ar
  end
  def run
    CreatePimAdImpressions.up if %w(development test).include?(deploy_env)
    import_all_impressions(ENV['AWS_S3_BUCKET'])
  end
end

at_exit do
  runner = ImportRunner.new
  if runner.run?
    runner.connect
    runner.run
  end
end
