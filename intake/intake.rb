load File.expand_path('../../config/boot.rb',__FILE__)
require File.expand_path('../../bin/aws_import',__FILE__)

require 'goliath'
require 'uber-s3'
require 'uber-s3/connection/em_http_fibered'

class Intake < Goliath::API
  use Goliath::Rack::Params
  use Goliath::Rack::DefaultMimeType
  use Goliath::Rack::Render, 'json'

  use Goliath::Rack::Validation::RequiredParam, {:key => 'pim_id', :type => 'ID'}
  use Goliath::Rack::Validation::NumericRange, {:key => 'pim_id', :min => 1}
  use Goliath::Rack::Validation::RequestMethod, %w(POST)           # allow POST requests only  
  
  def response(env)
    bucket = RawImpressions.new(ENV['AWS_S3_BUCKET'])
    bucket.create(1000, env['rack.input'])
    count = bucket.count
    [200, {},"Items: #{count}"]
  end
end
