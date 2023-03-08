require "./constants"
require "csv"

class QueryGenerator
    @db_api_production_db = ""
    @db_api_production = ""
    @bulk_import_query = ""

    def initialize(td_client,site,account_id,action_event,from_date, to_date, logger)
        puts "HEllo I am here"
        @td_client = td_client
        @site = site
        @account_id = account_id
        @action_event = action_event
        @from_date = from_date
        @to_date = to_date
        @db_api_production_db = ""
        @db_api_production = ""
        @query = ""
        @logger = logger
        populate_vars
    end

    #utils

    ##populate site specific variables such as DB names
    def populate_vars
        set_dbs
    end

    def set_dbs
        case @site
        when 'aws'
            @db_api_production_db = Constants::US_DB_API_PRODUCTION_DB
            @db_api_production = Constants::US_DB_API_PRODUCTION
        when 'eu01'
            @db_api_production_db = Constants::EU_DB_API_PRODUCTION_DB
            @db_api_production = Constants::EU_DB_API_PRODUCTION
        when 'aws-tokyo'
            @db_api_production_db = Constants::TOKYO_DB_API_PRODUCTION_DB
            @db_api_production = Constants::TOKYO_DB_API_PRODUCTION
        when 'ap02'
            @db_api_production_db = Constants::AP02_DB_API_PRODUCTION_DB
            @db_api_production = Constants::AP02_DB_API_PRODUCTION
        end
    end


    def generate_query
        case @action_event
        when 'bulk_import'
            @query = get_bulk_import_query
        when 'bulk_load'
            puts "to be implemented"
        when 'streaming_import'
            puts "to be implemented"
        end
    end
    
    def get_bulk_import_query
       bulk_import_query = "with bulk_imports_count as ("+
        "select TD_TIME_FORMAT(time, 'yyyy-MM', 'utc') m ,user_table_id, "+
                "sum(num_records) as total_import_record "+
            "from "+@db_api_production+".bulk_imports "+
            "where account_id="+@account_id+
            " and td_time_range(time, '"+@from_date+"', '"+@to_date+"') "+
            "and bulk_import_session_name not rlike ('^session_d{6,}') "+
        "group by 1,2"+
        "), "+
        "user_tables as ("+
        "SELECT table_id,"+
                "max_by(database_name, time) as database_name, "+
                "max_by(table_name, time) as table_name "+
            "from "+@db_api_production_db+".user_tables_history "+
            "where td_time_range(time, '"+@from_date+"', '"+@to_date+"') "+
            "and account_id="+@account_id+
        " group by 1 "+
        "UNION "+
        "SELECT table_id,"+
                "max_by(database_name, time) as database_name, "+
                "max_by(table_name, time) as table_name "+
            "from "+@db_api_production_db+".user_tables_deleted "+
            "where td_time_range(time, '"+@from_date+"', '"+@to_date+"') "+
            "and account_id="+@account_id+
        " group by 1 "+
        ") "+
        "select b.m, "+
                "u.database_name, "+
                "u.table_name, "+
                "b.total_import_record "+
            "from bulk_imports_count as b "+
                    "left outer join "+
                "user_tables as u "+
                    "on b.user_table_id = u.table_id "+
        "order by 2, 3, 1 "+
        ";"
        return bulk_import_query
    end

    def execute_query
        puts "Querying data"
        @logger.info("Executing the query - #{@query}")
        job = @td_client.query(@db_api_production,@query)
        puts job.job_id
        @logger.info("Job details #{job}")
        wait_job(job)
        job.update_status!  # get latest info
        job.result_each { |row| 
            CSV.open("./report/#{@action_event}_#{@account_id}.csv", "a") do |csv|
                csv << row
            end
        }

        job.job_result_format(job.job_id,'csv')
        

    end

    def wait_job(job)
        # wait for job to be finished
        cmdout_lines = 0
        stderr_lines = 0
        job.wait(nil, detail: true, verbose: true) do
          cmdout = job.debug['cmdout'].to_s.split("\n")[cmdout_lines..-1] || []
           stderr = job.debug['stderr'].to_s.split("\n")[stderr_lines..-1] || []
           (cmdout + stderr).each {|line|
             @logger.info(" ~~ #{line}")
           }
           cmdout_lines += cmdout.size
           stderr_lines += stderr.size
        end
    end
end