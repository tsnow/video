require 'aws-sdk'
# require 'uber-s3'
# Do this in callers to enable using the UberS3Adapter. 

# require File.expand_path('../raw_impressions/aws_adapter',__FILE__)
# require File.expand_path('../raw_impressions/uber_s3_adapter',__FILE__)



class RawImpressions
  attr_reader :adapter, :bucket
  def initialize(bucket_name)
    @adapter = s3_adapter(bucket_name)
  end
  def create(pim_id,file, now=Time.now.utc)
    raise ArgumentError.new("Not a pim_id: #{pim_id}") unless Integer(pim_id)
    raise ArgumentError.new("No file body supplied: #{file}") unless file && file.respond_to?(:read)
    begin
      data = file.read
      file.rewind
      json = JSON.parse(data)
      json = json.fetch('collection').fetch('items') # TODO: add test coverage
      raise ArgumentError.new(json) unless Array === json
    rescue => e
      json = nil
    end
    raise ArgumentError.new("File body is not Collection+JSON: Must contain root.collection.items[].") unless json
    name = ["raw-impressions",pim_id,now.strftime("%Y-%m-%d/%H:%M:%S.json")].join('/')

    adapter.store(name, file)
    name
  end
  class UberS3Adapter
    attr_reader :s3
    def initialize(bucket_name)
      @s3 =  UberS3.new({
                          :access_key         => ENV['AWS_ACCESS_KEY_ID'],
                          :secret_access_key  => ENV['AWS_SECRET_ACCESS_KEY'],
                          :bucket             => bucket_name,
                          :adapter            => :em_http_fibered
                        })
    end
    def store(name,file)
      s3.store(name, file.read)
    end
    def objects(prefix)
      s3.objects(prefix)
    end
  end
  class AWSAdapter
    attr_reader :bucket
    def initialize(bucket_name)
      s3 = AWS::S3.new
      @bucket = s3.buckets[bucket_name]
    end
    def store(name,file)
      bucket.objects[name].write(file)
    end
    def objects(prefix)
      bucket.objects.with_prefix(prefix)
    end

  end
  def s3_adapter(bucket_name)
    if defined?(::UberS3)
      @adapter = UberS3Adapter.new(bucket_name)
    else
      @adapter = AWSAdapter.new(bucket_name)
      @bucket = @adapter.bucket
    end
    @adapter
  end
  def each
    adapter.objects('raw-impressions/').each do |i|
      next if i.key[-1] == "/"
      # alternatively next if i.key[-4] != ".json" #if we want to ensure all files are json
      yield self,i
    end
  end
  def archive(i)
    new_name = i.key.sub('raw-impressions/','archived-impressions/')
    i.move_to(new_name)
  end
  def quarantine(i)
    new_name = i.key.sub('raw-impressions/','quarantined-impressions/')
    i.move_to(new_name)    
  end
  def log_errors(i, worked, errored)
    new_name = i.key.sub('raw-impressions/','impressions-errors/')
    bucket.objects.create(new_name,<<-END
worked: #{import.worked.count}
errored: #{import.errored.count}
error data: #{import.errored.to_yaml}
END
                          )
  end
  include Enumerable
end
