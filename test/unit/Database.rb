require 'test/unit'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class DatabaseTestCases < Test::Unit::TestCase
  def setup
    @db_file = 'c:/var/fbdata/testrbfb.fdb'
    @parms = {
      :database => "localhost:#{@db_file}",
      :username => 'sysdba',
      :password => 'masterkey',
      :charset => 'NONE',
      :role => 'READER' }
  end
  
  def test_new
    db = Database.new
    assert_instance_of Database, db
  end
  
  def test_properties_read
    db = Database.new
    assert_nil db.database
    assert_nil db.username
    assert_nil db.password
    assert_nil db.charset
    assert_nil db.role
  end

  def test_properties_write
    db = Database.new
    db.database = 'localhost:/var/fbdata/testrbfb.fdb'
    assert_equal 'localhost:/var/fbdata/testrbfb.fdb', db.database
    db.username = 'sysdba'
    assert_equal 'sysdba', db.username
    db.password = 'masterkey'
    assert_equal 'masterkey', db.password
    db.charset = 'NONE'
    assert_equal 'NONE', db.charset
    db.role = 'READER'
    assert_equal 'READER', db.role
  end
  
  def test_initialize
    db = Database.new(@parms)
    assert_equal 'localhost:c:/var/fbdata/testrbfb.fdb', db.database
    assert_equal 'sysdba', db.username
    assert_equal 'masterkey', db.password
    assert_equal 'NONE', db.charset
    assert_equal 'READER', db.role
  end
  
  def test_create_instance
    rm_rf @db_file
    db = Database.new(@parms)
    db.create
    assert File.exists?(@db_file)
  end
  
  def test_create_instance_block
    rm_rf @db_file
    db = Database.new(@parms)
    db.create do |connection|
      connection.execute("select * from RDB$DATABASE") do |cursor|
        row = cursor.fetch
        assert_instance_of Array, row
      end
      assert_equal 3, connection.dialect
      assert_equal 3, connection.db_dialect
    end
    assert File.exists?(@db_file)
  end
  
  def test_create_singleton
    rm_rf 'c:/var/fbdata/testrbfb.fdb'
    db = Database.create(@parms);
    assert File.exists?(@db_file)
  end

  def test_create_singleton_block
    rm_rf 'c:/var/fbdata/testrbfb.fdb'
    db = Database.create(@parms) do |connection|
      connection.execute("select * from RDB$DATABASE") do |cursor|
        row = cursor.fetch
        assert_instance_of Array, row
      end
    end
    assert_instance_of Database, db
    assert File.exists?(@db_file)
  end

  def test_connect_instance
    rm_rf @db_file
    db = Database.create(@parms)
    connection = db.connect
    begin
      assert_instance_of Connection, connection
      assert_equal 3, connection.dialect
      assert_equal 3, connection.db_dialect
    ensure
      connection.close
    end
  end

  def test_connect_singleton
    rm_rf @db_file
    db = Database.create(@parms)
    connection = Database.connect(@parms)
    begin
      assert_instance_of Connection, connection
      assert_equal 3, connection.dialect
      assert_equal 3, connection.db_dialect
    ensure
      connection.close
    end
  end
  
  def test_drop_instance
    rm_rf @db_file
    assert !File.exists?(@db_file)
    db = Database.create(@parms)
    assert File.exists?(@db_file)
    db.drop
    assert !File.exists?(@db_file)
  end
  
  def test_drop_singleton
    rm_rf @db_file
    assert !File.exists?(@db_file)
    Database.create(@parms)
    assert File.exists?(@db_file)
    Database.drop(@parms)
    assert !File.exists?(@db_file)
  end
end
    