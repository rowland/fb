require 'fileutils'
include FileUtils
require 'fb'

if RUBY_VERSION =~ /^2/
  require 'minitest/autorun'

  unless Minitest.const_defined?('Test')
    Minitest::Test = MiniTest::Unit::TestCase
  end

  class FbTestCase < Minitest::Test
  end

else
  require 'test/unit'

  class FbTestCase < Test::Unit::TestCase
    def default_test
    end
  end
end

module FbTestCases
  include Fb

  def setup
    @db_file = case RUBY_PLATFORM
      when /win32/ then 'c:/var/fbdata/drivertest.fdb'
      when /darwin/ then File.join(File.expand_path(File.dirname(__FILE__)), 'drivertest.fdb')
      else '/var/fbdata/drivertest.fdb'
    end
    @db_host = 'localhost'
    @username = 'sysdba'
    @password = 'masterkey'
    @parms = {
      :database => "#{@db_host}:#{@db_file}",
      :username => @username,
      :password => @password,
      :charset => 'NONE',
      :role => 'READER' }
    @parms_s = "database = #{@db_host}:#{@db_file}; username = #{@username}; password = #{@password}; charset = NONE; role = READER;"
    @fb_version = -1
    rm_rf @db_file

    Database.create(@parms) do |connection|
      
      d = connection.query("SELECT substring(rdb$get_context('SYSTEM', 'ENGINE_VERSION') from 1 for 1) from rdb$database")
      
      @fb_version = Integer(d.first[0])

      connection.drop
    end

    rm_rf @db_file
  end

end

class Fb::Connection
  def execute_script(sql_schema)
    self.transaction do
      sql_schema.strip.split(';').each do |stmt|
        self.execute(stmt);
      end
    end
  end
end
