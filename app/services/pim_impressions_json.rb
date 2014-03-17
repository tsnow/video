require File.expand_path('../../../app/services/impressions',__FILE__)
require File.expand_path('../../../app/utilities/pim_json',__FILE__)

class PimImpressionsJSON
  def self.upload_impressions(pim_id, body)
    imp=Impressions.new(pim_id,body)
    imp.publish
    return imp
  rescue => e
    imp.status = :failure
    imp.errors.push(e.to_s)
    return imp
  end

  def self.failure(failures)
    [500,{}, PimJson.new(:failure,failures).to_json]
  end
  def self.success(key)
    [200,{}, PimJson.new(:success,[], key).to_json]
  end
  def self.decline(errors)
    [400,{}, PimJson.new(:decline,errors).to_json]
  end


  def self.call(pim_id,body)
    imp = upload_impressions(pim_id,body)
    case imp.status
    when :success then
      success(imp.key)
    when :failure then
      failure(imp.errors)
    when :decline then
      decline(imp.errors)
    end
  end
end
