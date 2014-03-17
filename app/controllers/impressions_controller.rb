require Rails.root.join('intake','pim_impressions_json')
class ImpressionsController < ApplicationController
  before_filter :pim_id
  def create
    request.body.rewind
    status, headers, json = PimImpressionsJSON.call(params[:pim_id], request.body)
    render :json => json, :status => status
  end

end
