require File.expand_path("../test_helper", __FILE__)

class DatabaseTestCases < FbTestCase
  def setup
    super
    @parms = get_db_conn_params
    @reader = @parms.merge(:username => 'rubytest', :password => 'rubytest', :role => 'READER')
    @writer = @parms.merge(:username => 'rubytest', :password => 'rubytest', :role => 'WRITER')
    @database = @reader[:database]
    @db_file = @database.split(":", 2).last
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
    assert_equal @parms[:username], db.username
    assert_equal @parms[:password], db.password
    assert_equal 'NONE', db.charset
    assert_equal 'READER', db.role
  end

  def test_initialize_string
    params = @parms
    params_s = get_db_conn_string(params)
    db = Database.new params_s
    assert_equal @database, db.database
    assert_equal params[:username], db.username
    assert_equal params[:password], db.password
    assert_equal 'NONE', db.charset
    assert_equal 'READER', db.role
  end

  def test_create_instance
    db = Database.new(@parms)
    db.create
    assert File.exist?(@db_file)
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
    assert File.exist?(@db_file)
  end

  def test_create_singleton
    Database.create(@parms);
    assert File.exist?(@db_file)
  end

  def test_create_singleton_with_defaults
    Database.create(:database => @parms[:database]);
    assert File.exist?(@db_file)
  end

  def test_create_singleton_block
    db = Database.create(@parms) do |connection|
      connection.execute("select * from RDB$DATABASE") do |cursor|
        row = cursor.fetch
        assert_instance_of Array, row
      end
    end
    assert_instance_of Database, db
    assert File.exist?(@db_file)
  end

  def test_create_bad_param
    assert_raises TypeError do
      Database.create(1)
    end
  end

  def test_create_bad_page_size
    assert_raises Error do
      Database.create(@parms.merge(:page_size => 1000))
    end
  end

  def test_connect_instance
    db = Database.create(@parms)
    connection = db.connect
    assert_instance_of Connection, connection
    connection.close
  end

  def test_connect_singleton
    Database.create(@parms)
    connection = Database.connect(@parms)
    assert_instance_of Connection, connection
    connection.close
  end

  def test_drop_instance
    assert !File.exist?(@db_file)
    db = Database.create(@parms)
    assert File.exist?(@db_file)
    db.drop
    assert !File.exist?(@db_file)
  end

  def test_drop_singleton
    assert !File.exist?(@db_file)
    Database.create(@parms)
    assert File.exist?(@db_file)
    Database.drop(@parms)
    assert !File.exist?(@db_file)
  end

  def test_role_support
    Database.create(@parms) do |connection|
      connection.execute("create table test (id int, test varchar(10))")
      connection.execute("create role writer")
      connection.execute("grant all on test to writer")
      connection.execute("insert into test values (1, 'test role')")
    end

    connection = Database.connect(@parms)
    begin
      connection.execute("drop user rubytest")
      connection.commit
    rescue Error
    ensure
      connection.close rescue nil
    end

    Database.connect(@parms) do |connection|
      connection.execute("CREATE USER rubytest password 'rubytest'")
      connection.execute("GRANT WRITER TO rubytest")
      connection.commit
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
  end
end

