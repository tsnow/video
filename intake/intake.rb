load File.expand_path('../../config/boot.rb',__FILE__)
require File.expand_path('../../bin/aws_import',__FILE__)

require 'goliath'
require 'uber-s3'
require 'uber-s3/connection/em_http_fibered'

class Intake < Goliath::API
  
  def response(env)
    bucket = RawImpressions.new(ENV['AWS_S3_BUCKET'])
    bucket.create(1000, env['rack.input'])
    count = bucket.count
    [200, {},"Items: #{count}"]
  end
end
