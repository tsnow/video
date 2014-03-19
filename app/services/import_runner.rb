require File.expand_path('../../../app/services/impression_batch',__FILE__)
require File.expand_path('../../../app/models/raw_impressions',__FILE__)
require File.expand_path('../../../db/migrate/20140317000000_create_pim_ad_impressions',__FILE__)
require File.expand_path('../../../db/migrate/20140318211350_create_upload_files.rb',__FILE__)

class ImportRunner
  def process_raw_impressions(bucket,i, import=ImpressionBatch.new)
    #store a metadata file that includes # of rows that should have been entered into the db?
    items = JSON.parse(i.read)['collection']['items']
    i = bucket.begin_processing(i)
    puts "STORING IMPRESSIONS....."
    import.store_impressions(items)
    puts "FINISHED STORING IMPRESSIONS"
    begin
      if import.errors.empty? && import.worked.count == items.count
        puts "IMPORT SUCCEEDED, UPLOAD"
        @uf = UploadFile.create(:key => i.key, :bucket => i.bucket.try(:name), :etag => i.etag, :success => true)
        puts "IMPORT SUCCEEDED, ARCHIVE"
        bucket.archive(i)
      else
        puts "IMPORT FAILED, UPLOAD"
        @uf = UploadFile.create(:key => i.key, :bucket => i.bucket.try(:name), :etag => i.etag, :success => false)  
        puts "IMPORT FAILED, QUARANTINE"        
        bucket.quarantine(i)
        bucket.log_errors(i, import.worked, import.errors)
        #Circuit breaker?
      end
      @uf.update_attributes(:s3_connect_success => true)
    rescue => e # *should* only happen when an s3 outage occurs.
      # import.rollback
      # if we were able to import the entire file, we don't need to rollback
      # we just need to log s3 errors.  These files will be "stuck" in processing
      # so they won't try to reimport them
      puts "$$$$$$$$$$$$$$$$$$$$$$$$$"
      raise e
      puts "$$$$$$$$$$$$$$$$$$$$$$$$$$"
      @uf.update_attributes(:s3_connect_success => false)
      log_s3_failure(i,e)
      #bucket.log_errors(i, import.worked, import.errors) # Probably wouldn't work. Have to think about how to best handle these.
    end
    log_impression_processing(i,import)
  end
  
  def log_impressions_starting
    puts "raw_impressions_key,inserted,failed,total"
  end
  
  def log_impression_processing(i, import)
    puts "#{i.key.inspect},#{import.worked.count},#{import.errors.count},#{import.total.count}"
  end
  
  def log_s3_failure(i,e)
    $stderr.puts "communication_failure key=#{i.key.inspect} exception=#{e.class.to_s.inspect} message=#{e.inspect} backtrace=#{e.backtrace.inspect}"
  end
  
  def import_all_impressions(bucket_name)
    RawImpressions.new(bucket_name).each do |bucket, i|
      process_raw_impressions(bucket,i)
    end
  end
  
  
  def run?
    File.expand_path($0) == File.expand_path(__FILE__)
  end
  
  def ar_config
    db_yml = if defined?(Rails)
      Rails.root.join('config/database.yml')
    else
      File.expand_path('../../../config/database.yml',__FILE__)
    end
    YAML::load(ERB.new(File.read(db_yml)).result(binding))
  end
  
  def connect_aws
    
    required_env = %w(AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_S3_BUCKET)
    
    fail("Please supply #{required_env.join(', ')}") unless required_env.all?{|i|ENV[i]}
    
    AWS.config(
               :access_key_id => ENV['AWS_ACCESS_KEY_ID'],
               :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'])
  end
  def connect_ar
    require 'erb'
    ActiveRecord::Base.configurations= ar_config
    ActiveRecord::Base.establish_connection(ActiveRecord::Base.configurations[deploy_env])
  end
  def deploy_env
    ENV['RAILS_ENV'] || 'development'
  end
  def connect
    connect_aws
    connect_ar
  end
  def run
    begin
      AWS::S3.new.buckets.to_a
    rescue => e
      $stderr.puts "ImportRunner: Cannot even get the list of buckets from S3. Please make sure ImportRunner#connect has been run. Access Key ID given: #{AWS.config.access_key_id}"
      raise
    end
    CreatePimAdImpressions.up if %w(development test).include?(deploy_env)
    CreateUploadFiles.up if %w(development test).include?(deploy_env)
    import_all_impressions(ENV['AWS_S3_BUCKET'])
  end
end
