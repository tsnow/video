require File.expand_path('../../../app/models/raw_impressions',__FILE__)
class Impressions
  attr_reader :errors,:status, :key
  def initialize(pim_id, body)
    @errors = []
    @pim_id = pim_id
    @status = :decline
    return unless body
    @body = body
    @body.rewind
  end
  def publish
    @key = RawImpressions.new(ENV['AWS_S3_BUCKET']).create(@pim_id, @body)
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
