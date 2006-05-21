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
    rm_rf @db_file
  end
  
  def test_transaction
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
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      connection.execute("create table test (id int, name varchar(20));");
      #connection.execute("select * from rdb$database;");
      assert connection.transaction_started
      connection.commit
      assert !connection.transaction_started
    end
    Database.drop(@parms)
  end

  def test_dialects
    db = Database.create(@parms) do |connection|
      assert_equal 3, connection.dialect
      assert_equal 3, connection.db_dialect
    end
  end
  
  def test_open?
    db = Database.new(@parms);
    db.create
    connection = db.connect
    assert connection.open?
    connection.close
    assert !connection.open?
    db.drop
  end
  
  def test_properties
    Database.create(@parms) do |connection|
      assert_equal @parms[:database], connection.database
      assert_equal @parms[:username], connection.username
      assert_equal @parms[:password], connection.password
      assert_equal @parms[:role], connection.role
      assert_equal @parms[:charset], connection.charset
    end
  end
  
  def test_to_s
    db = Database.new(@parms)
    db.create
    connection = db.connect
    begin
      assert_equal "#{@parms[:database]} (OPEN)", connection.to_s
    ensure
      connection.close
      assert_equal "#{@parms[:database]} (CLOSED)", connection.to_s
    end
  end
end
