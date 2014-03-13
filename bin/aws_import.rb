#!/usr/bin/env ruby
require 'aws-sdk'
require 'active_record'
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
  
      

class PimAdImpressions < ActiveRecord::Base
end

class RawImpressions
  attr_reader :bucket
  def initialize(bucket_name)
    s3 = AWS::S3.new
    bucket = s3.buckets[bucket_name]
    @bucket = bucket
  end
  def each
    bucket.objects.with_prefix('raw-impressions/').each do |i|
      yield self,i
    end
  end
  def archive(i)
    new_name = i.key.sub('raw-impressions/','archived-impressions/')
    i.move_to(new_name)
  end
  include Enumerable
end

def process_raw_impressions(bucket,i)
    bucket.archive(i)
end
def import_all_impressions(bucket_name)
  RawImpressions.new(bucket_name).each do|bucket, i|
    process_raw_impressions(bucket,i)
  end
end
at_exit do
  if $1 === __FILE__

    required_env = %w(AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_S3_BUCKET)

    fail("Please supply #{required_env.join(', ')}") unless required_env.all?{|i|ENV[i]}

    AWS.config(
           :access_key_id => ENV['AWS_ACCESS_KEY_ID'],
           :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'])


require 'erb'
ActiveRecord::Base.configurations= YAML::load(ERB.new(File.read(File.expand_path('../config/database.yml',__FILE__))).result(binding))
ActiveRecord::Base.establish_connection(ActiveRecord::Base.configurations[ENV['RAILS_ENV'] ||'development'])

    CreatePimAdImpressions.up if %w(development test).include?(ENV['RAILS_ENV'])
    import_all_impressions(ENV['AWS_S3_BUCKET'])
    
  end
end
