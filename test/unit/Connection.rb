require 'test/unit'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class ConnectionTestCases < Test::Unit::TestCase
  def setup
    @db_file = 'c:/var/fbdata/testrbfb.fdb'
    @parms = {
      :database => "localhost:#{@db_file}",
      :username => 'sysdba',
      :password => 'masterkey',
      :charset => 'NONE',
      :role => 'READER' }
  end
  
  def test_transaction
    rm_rf @db_file
    Database.create(@parms) do |connection|
      n = 0
      assert !connection.transaction_started
      connection.transaction
      assert connection.transaction_started
      connection.commit
      assert !connection.transaction_started
      connection.transaction
      assert connection.transaction_started
      connection.rollback
      assert !connection.transaction_started
    end
    Database.drop(@parms)
  end
  
  def test_execute
    rm_rf @db_file
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      #connection.execute("create table test (id int, name varchar(20));");
      connection.execute("select * from rdb$database;");
      assert connection.transaction_started
      connection.commit
      assert !connection.transaction_started
    end
    Database.drop(@parms)
  end
end
