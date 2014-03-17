require 'multi_json'

class PimJson
  def initialize(status,errors, key=nil)
    @status = status
    @errors = errors
    @key = key
  end
  def error_clause
    return {} if @errors.empty?
    return {"errors" => @errors}
  end
  def result_clause
    status = {"status"=> @status.to_s}
    key = {}
    key = {'key' => @key} if @key
    {"result"=>status.merge(key)}
  end
  def to_json
    MultiJson.dump(result_clause.merge(error_clause))
  end
end
