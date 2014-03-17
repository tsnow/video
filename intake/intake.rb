load File.expand_path('../../config/boot.rb',__FILE__)
require File.expand_path('../../bin/aws_import',__FILE__)

require 'goliath'
require 'multi_json'
require 'uber-s3'
require 'uber-s3/connection/em_http_fibered'




class PimJson
  def initialize(status,errors)
    @status = status
    @errors = errors
  end
  def error_clause
    return {} if @errors.empty?
    return {"errors" => @errors}
  end
  def result_clause
    {"result"=>{"status"=> @status.to_s}}
  end
  def to_json
    MultiJson.dump(result_clause.merge(error_clause))
  end
end

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
    @errors.push(e.to_s)
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

  def failure(failures)
    [500,{}, PimJson.new(:failure,failures).to_json]
  end
  def success
    [200,{}, PimJson.new(:success,[]).to_json]
  end
  def decline(errors)
    [400,{}, PimJson.new(:decline,errors).to_json]
  end

  def response(env)
    status, errors = upload_impressions(env)
    case status
    when :success then
      success
    when :failure then
      failure(errors)
    when :decline then
      decline(errors)
    end
  end
end
