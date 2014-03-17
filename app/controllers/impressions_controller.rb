require Rails.root.join('app','services','pim_impressions_json')
class ImpressionsController < ApplicationController
skip_before_filter :verify_authenticity_token
  rescue_from StandardError do |exception|
    rescue_not_found(exception)
  end

  def create
    request.body.rewind
    body = request.body.instance_variable_get(:@input)

    status, headers, json = PimImpressionsJSON.call(params[:pim_id], body)
    render :json => json, :status => status
  end
  def unimplemented
    rescue_not_found("No behaviour is defined for #{request.method} #{params[:dir]}")
  end
  protected
  def rescue_not_found(exception)
    status, headers, json = PimImpressionsJSON.decline([exception.to_s])
    render :json => json, :status => status
  end

end
