#!/usr/bin/env ruby
require 'aws-sdk'
def import_all_impressions(bucket_name)
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

    import_all_impressions(ENV['AWS_S3_BUCKET'])
    
  end
end
