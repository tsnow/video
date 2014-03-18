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
    # rollback
    # not sure why we are rolling back here, we're in a transaction already
    error(@current, e) 
    return self
  end
  
  
  def store_impression(i)
    i['played_at'] = Time.parse(i['played_at']).utc
    # don't want to duplicate imports
    existing = PimAdImpression.where(:pim_id => i['pim_id'], :played_at => i['played_at'], :campaign_element_id => i['campaign_element_id'])
    if existing.present?
      dupe i
    else
      imp = PimAdImpression.create(i)
      if imp.errors.present?
        error i, imp.errors
      else
        push imp
      end 
    end
  rescue => e
    error i, e
  end
end
