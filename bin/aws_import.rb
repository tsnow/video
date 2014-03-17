#!/usr/bin/env ruby
require 'active_record'
require 'aws-sdk'
require 'multi_json'
require File.expand_path('../../app/services/import_runner',__FILE__)

require 'logger'
AWS.config(:logger => Logger.new($stdout), :log_level => :debug)




at_exit do
  runner = ImportRunner.new
  if runner.run?
    runner.connect
    runner.run
  end
end

