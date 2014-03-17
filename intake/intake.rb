load File.expand_path('../../config/boot.rb',__FILE__)
require File.expand_path('../../bin/aws_import',__FILE__)

require 'goliath'
require 'uber-s3'
require 'uber-s3/connection/em_http_fibered'
class Impressions
  attr_reader :errors,:status
  def initialize(pim_id, body)
    @errors = []
    @pim_id = pim_id
    @status = :decline
    return unless body
    @body = body
    @body.rewind
  end
  def publish
    RawImpressions.new(ENV['AWS_S3_BUCKET']).create(@pim_id, @body)
    @status = :success
  rescue ArgumentError => e
    @errors.push(e)
    @status = :decline
  rescue => e
    # TODO:
    # write to some log or newrelic some where
    case
    when e.respond_to?(:http_response) then
      @errors.push(e.http_response.body)
    else
      @errors.push(e.to_s)
    end
    @status = :failure
  end
end


class Intake < Goliath::API
  use Goliath::Rack::Params
  use Goliath::Rack::DefaultMimeType
  use Goliath::Rack::Render, 'json'

  use Goliath::Rack::Validation::RequiredParam, {:key => 'pim_id', :type => 'ID'}
  use Goliath::Rack::Validation::NumericRange, {:key => 'pim_id', :min => 1}
  use Goliath::Rack::Validation::RequestMethod, %w(POST)           # allow POST requests only  
  
  def upload_impressions(env)
    imp=Impressions.new(params['pim_id'], env['rack.input'])
    imp.publish
    return [imp.status,imp.errors]
  rescue => e
    return [:failure, imp.errors.push(e.to_s)]
  end
  def response(env)
    bucket = RawImpressions.new(ENV['AWS_S3_BUCKET'])
    count = bucket.count
    status, errors = upload_impressions(env)
    [200, {},"Items: #{count}"]
  end
end
