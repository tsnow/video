#!/usr/bin/env ruby
require 'aws-sdk'

AWS.config(
           :access_key_id => ENV['AWS_ACCESS_KEY'],
           :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'])
    s3 = AWS::S3.new
    bucket = s3.buckets['rips-assets-development']
    bucket.objects.with_prefix('raw-impressions/').each do |i|
    new_name = i.key.sub('raw-impressions/','archived-impressions/')
    i.move_to(new_name)
  end
