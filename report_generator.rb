#!/usr/bin/env ruby
require "stringio"
require "td-client"
require "csv"
require "Logger"
require "./query_generator"

class ReportGenerator
  def initialize(input_file)
    @td_api_key = ENV["TD_API_KEY"] ||= ""
    @td_env = ENV["TD_ENV"] ||= ""
    @account_id = ""
    @account_site = ""
    @action_event = ""
    @from_date = ""
    @to_date = ""
    @qg_inst = ""
    @client = TreasureData::Client.new(@td_api_key)
    @suffix = Time.now.utc.iso8601.gsub(/[ :]+/, "-")
    @logger = Logger.new("log/report_generator_logs_#{@suffix}.log")
    puts "Log file: log/report_generator_logs_#{@suffix}.log"
    @logger.info("api-key: #{@td_api_key}")
    @logger.info("client: #{@client}")
    @input_data = CSV.parse(File.read(input_file), headers: true)
    puts @input_data[0]["account"]
    read_input_data
  end

  # Utils

  ##read data inputs from the input file
  def read_input_data
    @account_id = @input_data[0]["account"].split(':')[1]
    @account_site = @input_data[0]["account"].split(':')[0]
    @action_event = @input_data[0]["action_event"]
    @from_date = @input_data[0]["from_date"]
    @to_date = @input_data[0]["to_date"]
  end

  ##is connected to the correct end-point##
  def check_account
    case @account_site
    when 'aws'
      if 'https://api.treasuredata.com' != @td_env
        @logger.info("Please switch your TD account to access US endpoint and try again")
        return false
      else
        @logger.info("You are in the right account. Proceeding with next steps..")
        return true
      end
    when 'eu01'
      if 'https://api.eu01.treasuredata.com' != @td_env
        @logger.info("Please switch your TD account to access EU endpoint and try again")
        return false
      else
        @logger.info("You are in the right account. Proceeding with next steps..")
        return true
      end
    when 'aws-tokyo'
      if 'https://api.treasuredata.co.jp' != @td_env
        @logger.info("Please switch your TD account to access TOKYO endpoint and try again")
        return false
      else
        @logger.info("You are in the right account. Proceeding with next steps..")
        return true
      end
    when 'ap02'
      if 'https://api.ap02.treasuredata.com' != @td_env
        @logger.info("Please switch your TD account to access KOREA endpoint and try again")
        return false
      else
        @logger.info("You are in the right account. Proceeding with next steps..")
        return true
      end
    end
  end

  ##extract query matching to the action event##
  def generate_query
    @qg_inst = QueryGenerator.new(@client,@account_site,@account_id,@action_event,@from_date,@to_date,@logger)
    @qg_inst.generate_query

    #@query = @qg_inst.get_bulk_import_query(@td_env)
    puts @action_event
  end

  def execute_query
    @qg_inst.execute_query
  end

  ##reset variables##
  def reset_vars
    @account_id = ""
    @account_site = ""
    @action_event = ""
    @from_date = ""
    @to_date = ""
  end
end
  
  
  if __FILE__ == $0
    puts "start"
    api_key = ENV["TD_API_KEY"] ||= ""
    td_env = ENV["TD_ENV"] ||=""
    puts ARGV[0]
    rg = ReportGenerator.new(ARGV[0])
    if(rg.check_account)
      rg.generate_query
      rg.execute_query
    else
      #rg.reset_vars
    end
  end