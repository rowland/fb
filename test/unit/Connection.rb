require 'test/unit'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class ConnectionTestCases < Test::Unit::TestCase
  def setup
    @db_file = 'c:/var/fbdata/drivertest.fdb'
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
      connection.drop
    end
  end
  
  def test_execute
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      connection.execute("create table test (id int, name varchar(20));");
      #connection.execute("select * from rdb$database;");
      assert connection.transaction_started
      connection.commit
      assert !connection.transaction_started
      connection.drop
    end
  end
  
  def test_insert_commit
    sql_schema = "create table test (id int, name varchar(20))"
    sql_insert = "insert into test (id, name) values (?, ?)"
    sql_select = "select * from test order by id"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.commit;
      10.times do |i|
        connection.execute(sql_insert, i, i.to_s);
      end
      connection.commit
      connection.execute(sql_select) do |cursor|
        rows = cursor.fetchall
        assert 10, rows.size
        10.times do |i|
          assert i, rows[i][0]
          assert i.to_s, rows[i][1]
        end
      end
      connection.drop
    end
  end
  
  def test_insert_rollback
    sql_schema = "create table test (id int, name varchar(20))"
    sql_insert = "insert into test (id, name) values (?, ?)"
    sql_select = "select * from test order by id"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.commit;
      10.times do |i|
        connection.execute(sql_insert, i, i.to_s);
      end
      connection.rollback
      connection.execute(sql_select) do |cursor|
        rows = cursor.fetchall
        assert 0, rows.size
      end
      connection.drop
    end
  end
  
  def test_insert_blobs_text
    sql_schema = "create table test (id int, name varchar(20), memo blob sub_type text)"
    sql_insert = "insert into test (id, name, memo) values (?, ?, ?)"
    sql_select = "select * from test order by id"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.commit;
      memo = IO.read("fb.c")
      assert memo.size > 50000
      10.times do |i|
        connection.execute(sql_insert, i, i.to_s, memo);
      end
      connection.commit
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :hash do |row|
          assert_equal i, row["ID"]
          assert_equal i.to_s, row["NAME"]
          assert_equal memo, row["MEMO"]
          i += 1
        end
      end
      connection.drop
    end
  end

  def test_insert_blobs_binary
    sql_schema = "create table test (id int, name varchar(20), attachment blob segment size 1000)"
    sql_insert = "insert into test (id, name, attachment) values (?, ?, ?)"
    sql_select = "select * from test order by id"
    #filename = "data.dat"
    filename = "fb.c"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.commit;
      attachment = File.open(filename,"rb") do |f|
        f.read * 3
      end
      assert (attachment.size > 150000), "Not expected size"
      3.times do |i|
        connection.execute(sql_insert, i, i.to_s, attachment);
      end
      connection.commit
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :array do |row|
          assert_equal i, row[0], "ID's do not match"
          assert_equal i.to_s, row[1], "NAME's do not match"
          assert_equal attachment.size, row[2].size, "ATTACHMENT sizes do not match"
          i += 1
        end
      end
      connection.drop
    end
  end

  def test_dialects
    db = Database.create(@parms) do |connection|
      assert_equal 3, connection.dialect
      assert_equal 3, connection.db_dialect
      connection.drop
    end
  end
  
  def test_open?
    db = Database.create(@parms);
    connection = db.connect
    assert connection.open?
    connection.close
    assert !connection.open?
    db.drop
  end
  
  def test_properties_instance
    db = Database.new(@parms)
    db.create
    db.connect do |connection|
      assert_equal @parms[:database], connection.database
      assert_equal @parms[:username], connection.username
      assert_equal @parms[:password], connection.password
      assert_equal @parms[:role], connection.role
      assert_equal @parms[:charset], connection.charset
      connection.drop
    end
  end
  
  def test_properties_singleton
    Database.create(@parms) do |connection|
      assert_equal @parms[:database], connection.database
      assert_equal @parms[:username], connection.username
      assert_equal @parms[:password], connection.password
      assert_equal @parms[:role], connection.role
      assert_equal @parms[:charset], connection.charset
      connection.drop
    end
  end
  
  def test_drop_instance
    db = Database.create(@parms)
    assert File.exists?(@db_file)
    connection = db.connect
    assert connection.open?    
    connection.drop
    assert !connection.open?
    assert !File.exists?(@db_file)
  end
  
  def test_drop_singleton
    Database.create(@parms) do |connection|
      assert File.exists?(@db_file)
      connection.drop
      assert !File.exists?(@db_file)
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
  
  def test_table_names
    $sql_schema = <<-END
      create table test1 (id int);
      create table test2 (id int);
    END
    Database.create(@parms) do |connection|
      $sql_schema.strip.split(';').each do |stmt|
        connection.execute(stmt);
      end
      connection.commit
      table_names = connection.table_names
      assert_equal 'TEST1', table_names[0]
      assert_equal 'TEST2', table_names[1]
    end
  end

  def test_generator_names
    $sql_schema = <<-END
      create generator test1_seq;
      create generator test2_seq;
    END
    Database.create(@parms) do |connection|
      $sql_schema.strip.split(';').each do |stmt|
        connection.execute(stmt);
      end
      connection.commit
      names = connection.generator_names
      assert_equal 'TEST1_SEQ', names[0]
      assert_equal 'TEST2_SEQ', names[1]
    end
  end
end
