load File.expand_path('../../config/boot.rb',__FILE__)

require 'goliath'
require File.expand_path('../../app/services/pim_impressions_json',__FILE__)
require 'uber-s3'
require 'uber-s3/connection/em_http_fibered'

class Intake < Goliath::API
  use Goliath::Rack::Params
  use Goliath::Rack::DefaultMimeType

  use Goliath::Rack::Validation::RequiredParam, {:key => 'pim_id', :type => 'ID'}
  use Goliath::Rack::Validation::NumericRange, {:key => 'pim_id', :min => 1}
  use Goliath::Rack::Validation::RequestMethod, %w(POST)           # allow POST requests only  
  
  def response(env)
    PimImpressionsJSON.call(params['pim_id'], env['rack.input'])
  end
end
