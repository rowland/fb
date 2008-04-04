require 'fileutils'
include FileUtils

module FbTestCases
  def setup
    @db_file = case RUBY_PLATFORM
      when /win32/ then 'c:/var/fbdata/drivertest.fdb'
      when /darwin/ then '/var/fbdata/drivertest.fdb'
      else '/var/fbdata/drivertest.fdb'
    end
    @db_host = 'localhost'
    @parms = {
      :database => "#{@db_host}:#{@db_file}",
      :username => 'sysdba',
      :password => 'masterkey',
      :charset => 'NONE',
      :role => 'READER' }
    @parms_s = "database = #{@db_host}:#{@db_file}; username = sysdba; password = masterkey; charset = NONE; role = READER;"
    rm_rf @db_file
  end
end

require 'fb'

class Fb::Connection
  def execute_script(sql_schema)
    self.transaction do
      sql_schema.strip.split(';').each do |stmt|
        self.execute(stmt);
      end
    end
  end
end
