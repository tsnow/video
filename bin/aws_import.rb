#!/usr/bin/env ruby
require 'active_record'
require 'aws-sdk'
require 'multi_json'

require 'logger'
AWS.config(:logger => Logger.new($stdout), :log_level => :debug)



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

class ImpressionBatch < Batch
  def store_impressions(impressions)
    ActiveRecord::Base.transaction do
      impressions.each do |i|
        @current = i
        store_impression(i)
      end
    end
    
    return self
  rescue => e 
    rollback
    error(@current, e) 
    return self
  end
  
  
  def store_impression(i)
    i['played_at'] = Time.parse(i['played_at']).utc
    imp = PimAdImpression.create(i)
    if imp.errors.present?
      error i, imp.errors
    else
      push imp
    end 
  rescue => e
    error i, e
  end
end

class PimAdImpression < ActiveRecord::Base
end

class RawImpressions
  attr_reader :adapter, :bucket
  def initialize(bucket_name)
    @adapter = s3_adapter(bucket_name)
  end
  def create(pim_id,file, now=Time.now.utc)
    raise ArgumentError.new("Not a pim_id: #{pim_id}") unless Integer(pim_id)
    raise ArgumentError.new("No file body supplied: #{file}") unless file && file.respond_to?(:read)
    raise ArgumentError.new("File body is not JSON") unless JSON.parse(file.read) && file.rewind
    name = ["raw-impressions",pim_id,now.strftime("%Y-%m-%d/%H:%m%s.json")].join('/')

    adapter.store(name, file)
  end
  class UberS3Adapter
    attr_reader :s3
    def initialize(bucket_name)
      @s3 =  UberS3.new({
                          :access_key         => ENV['AWS_ACCESS_KEY_ID'],
                          :secret_access_key  => ENV['AWS_SECRET_ACCESS_KEY'],
                          :bucket             => bucket_name,
                          :adapter            => :em_http_fibered
                        })
    end
    def store(name,file)
      s3.store(name, file.read)
    end
    def objects(prefix)
      s3.objects(prefix)
    end
  end
  class AWSAdapter
    attr_reader :bucket
    def initialize(bucket_name)
      s3 = AWS::S3.new
      @bucket = s3.buckets[bucket_name]
    end
    def store(name,file)
      bucket.objects[name].write(file)
    end
    def objects(prefix)
      bucket.objects.with_prefix(prefix)
    end

  end
  def s3_adapter(bucket_name)
    if defined?(::UberS3)
      @adapter = UberS3Adapter.new(bucket_name)
    else
      @adapter = AWSAdapter.new(bucket_name)
      @bucket = @adapter.bucket
    end
    @adapter
  end
  def each
    adapter.objects('raw-impressions/').each do |i|
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
  def process_raw_impressions(bucket,i, import=ImpressionBatch.new)
    #store a metadata file that includes # of rows that should have been entered into the db?
    items = JSON.parse(i.read)['collection']['items']
    
    import.store_impressions(items)
    begin
      if import.errored.empty? && import.worked.count == items.count
        bucket.archive(i)
      else
        #if a particular file fails, we want to log the errors
        #we probably want to move the file to a different dir, but let's leave it here now
        new_name = i.key.sub('raw-impressions/','impressions-errors/')
        bucket.errchive(i,<<-END
worked: #{import.worked.count}
errored: #{import.errored.count}
error data: #{import.errored.to_yaml}
END
                        )
        #Circuit breaker?
      end
    rescue => e # *should* only happen when an s3 outage occurs.
      import.rollback
      log_s3_failure(i,e)
    end
    log_impression_processing(i,import)
  end
  
  def log_impressions_starting
    puts "raw_impressions_key,inserted,failed,total"
  end
  
  def log_impression_processing(i, import)
    puts "#{i.key.inspect},#{import.worked.count},#{import.errored.count},#{import.total.count}"
  end
  
  def log_s3_failure(i,e)
    $stderr.puts "communication_failure key=#{i.key.inspect} exception=#{e.class.to_s.inspect} message=#{e.inspect} backtrace=#{e.backtrace.inspect}"
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

