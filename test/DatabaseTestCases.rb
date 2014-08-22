require 'test/FbTestCases'

class DatabaseTestCases < FbTestCase
  include FbTestCases
  
  def setup
    super
    @database = "localhost:#{@db_file}"
    @reader = {
      :database => "localhost:#{@db_file}",
      :username => 'rubytest',
      :password => 'rubytest',
      :charset => 'NONE',
      :role => 'READER' }
    @writer = {
      :database => "localhost:#{@db_file}",
      :username => 'rubytest',
      :password => 'rubytest',
      :charset => 'NONE',
      :role => 'WRITER' }
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
    db.database = @database
    assert_equal @database, db.database
    db.username = 'sysdba'
    assert_equal 'sysdba', db.username
    db.password = 'masterkey'
    assert_equal 'masterkey', db.password
    db.charset = 'NONE'
    assert_equal 'NONE', db.charset
    db.role = 'READER'
    assert_equal 'READER', db.role
  end
  
  def test_initialize_hash
    db = Database.new(@parms)
    assert_equal @database, db.database
    assert_equal @username, db.username
    assert_equal @password, db.password
    assert_equal 'NONE', db.charset
    assert_equal 'READER', db.role
  end
  
  def test_initialize_string
    db = Database.new(@parms_s)
    assert_equal @database, db.database
    assert_equal @username, db.username
    assert_equal @password, db.password
    assert_equal 'NONE', db.charset
    assert_equal 'READER', db.role
  end
  
  def test_create_instance
    db = Database.new(@parms)
    db.create
    assert File.exists?(@db_file)
  end
  
  def test_create_instance_block
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
    db = Database.create(@parms);
    assert File.exists?(@db_file)
  end

  def test_create_singleton_with_defaults
    db = Database.create(:database => "localhost:#{@db_file}");
    assert File.exists?(@db_file)
  end

  def test_create_singleton_block
    db = Database.create(@parms) do |connection|
      connection.execute("select * from RDB$DATABASE") do |cursor|
        row = cursor.fetch
        assert_instance_of Array, row
      end
    end
    assert_instance_of Database, db
    assert File.exists?(@db_file)
  end
  
  def test_create_bad_param
    assert_raises TypeError do
      db = Database.create(1)
    end
  end

  def test_create_bad_page_size
    assert_raises Error do
      db = Database.create(@parms.merge(:page_size => 1000))
    end
  end

  def test_connect_instance
    db = Database.create(@parms)
    connection = db.connect
    assert_instance_of Connection, connection
    connection.close
  end

  def test_connect_singleton
    db = Database.create(@parms)
    connection = Database.connect(@parms)
    assert_instance_of Connection, connection
    connection.close
  end
  
  def test_drop_instance
    assert !File.exists?(@db_file)
    db = Database.create(@parms)
    assert File.exists?(@db_file)
    db.drop
    assert !File.exists?(@db_file)
  end
  
  def test_drop_singleton
    assert !File.exists?(@db_file)
    Database.create(@parms)
    assert File.exists?(@db_file)
    Database.drop(@parms)
    assert !File.exists?(@db_file)
  end
  
  def test_role_support
    Database.create(@parms) do |connection|
      connection.execute("create table test (id int, test varchar(10))")
      connection.execute("create role writer")
      connection.execute("grant all on test to writer")
      connection.execute("grant writer to rubytest")
      connection.commit
      connection.execute("insert into test values (1, 'test role')")
    end
    Database.connect(@reader) do |connection|
      assert_raises Error do
        connection.execute("select * from test") do |cursor|
          flunk "Should not reach here."
        end
      end
    end
    Database.connect(@writer) do |connection|
      connection.execute("select * from test") do |cursor|
        row = cursor.fetch :hash
        assert_equal 1, row["ID"]
        assert_equal 'test role', row["TEST"]
      end
    end
    Database.drop(@parms)
  end
end
    
