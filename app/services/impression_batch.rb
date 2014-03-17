require File.expand_path('../../../app/utilities/batch',__FILE__)
require File.expand_path('../../../app/models/pim_ad_impression',__FILE__)

class ImpressionBatch < Batch
  def store_impressions(impressions)
    ActiveRecord::Base.transaction do
      impressions.each do |i|
        @current = i
        store_impression(i)
      end
    end
    
    return self
  rescue => e 
    rollback
    error(@current, e) 
    return self
  end
  
  
  def store_impression(i)
    i['played_at'] = Time.parse(i['played_at']).utc
    imp = PimAdImpression.create(i)
    if imp.errors.present?
      error i, imp.errors
    else
      push imp
    end 
  rescue => e
    error i, e
  end
end
