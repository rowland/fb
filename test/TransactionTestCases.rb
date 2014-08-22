require 'test/FbTestCases'

class TransactionTestCases < FbTestCase
  include FbTestCases
  
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

  def test_transaction_block
    Database.create(@parms) do |connection|
      n = 0
      assert !connection.transaction_started
      connection.transaction do
        assert connection.transaction_started
      end
      assert !connection.transaction_started
      assert_raises RuntimeError do
        connection.transaction do
          assert connection.transaction_started
          raise "generic exception"
        end
      end
      assert !connection.transaction_started
      connection.drop
    end
  end

  def test_auto_transaction_select_with_exception
    sql_select = "SELECT * FROM RDB$DATABASE"
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      assert_raises RuntimeError do
        connection.execute(sql_select) do |cursor|
          assert connection.transaction_started
          raise "abort"
        end
      end
      assert !connection.transaction_started
      connection.drop
    end
  end

  def test_auto_transaction_insert_with_exception
    sql_schema = "CREATE TABLE TEST (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      assert !connection.transaction_started
      connection.execute(sql_insert, 1, "one")
      assert !connection.transaction_started
      assert_raises Error do
        connection.execute(sql_insert, 1, "two")
      end
      assert !connection.transaction_started, "transaction is active"
      connection.drop
    end
  end

  def test_auto_transaction_query
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      rs = connection.query("select * from rdb$database")
      assert !connection.transaction_started
      connection.drop
    end
  end

  def test_query_in_transaction
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      connection.transaction do
        assert connection.transaction_started
        rs = connection.query("select * from rdb$database")
        assert connection.transaction_started
      end
      assert !connection.transaction_started
      connection.drop
    end
  end

  def test_insert_commit
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.transaction
      10.times do |i|
        connection.execute(sql_insert, i, i.to_s);
      end
      connection.commit
      connection.execute(sql_select) do |cursor|
        rows = cursor.fetchall
        assert_equal 10, rows.size
        10.times do |i|
          assert_equal i, rows[i][0]
          assert_equal i.to_s, rows[i][1]
        end
      end
      connection.drop
    end
  end

  def test_insert_rollback
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.transaction
      10.times do |i|
        connection.execute(sql_insert, i, i.to_s);
      end
      connection.rollback
      rows = connection.execute(sql_select) do |cursor| cursor.fetchall end
      assert_equal 0, rows.size
      connection.drop
    end
  end

  def test_transaction_block_insert_commit
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      assert !connection.transaction_started
      result = connection.transaction do
        assert connection.transaction_started
        10.times do |i|
          connection.execute(sql_insert, i, i.to_s);
        end
        assert connection.transaction_started
        "transaction block result"
      end
      assert_equal "transaction block result", result
      assert !connection.transaction_started
      connection.execute(sql_select) do |cursor|
        assert_equal 10, cursor.fetchall.size
      end
      connection.drop
    end
  end

  def test_transaction_block_insert_rollback
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      assert !connection.transaction_started
      assert_raises RuntimeError do
        connection.transaction do
          10.times do |i|
            connection.execute(sql_insert, i, i.to_s);
          end
          raise "Raise an exception, causing the transaction to be rolled back."
        end
      end
      rows = connection.execute(sql_select) do |cursor| cursor.fetchall end
      assert_equal 0, rows.size
      connection.drop
    end
  end

  def test_simultaneous_transactions
    db_file1 = "#{@db_file}1"
    db_file2 = "#{@db_file}2"
    rm_rf db_file1
    rm_rf db_file2
    parms1 = @parms.merge(:database => "#{@db_host}:#{db_file1}")
    parms2 = @parms.merge(:database => "#{@db_host}:#{db_file2}")
    Database.create(parms1) do |conn1|
      Database.create(parms2) do |conn2|
        assert !conn1.transaction_started, "conn1 transaction is started"
        assert !conn2.transaction_started, "conn2 transaction is started"
        conn1.transaction do
          assert conn1.transaction_started, "conn1 transaction is not started"
          assert !conn2.transaction_started, "conn2 transaction is started"
          conn2.transaction do
            assert conn2.transaction_started, "conn2 transaction is not started"
            assert conn1.transaction_started, "conn1 transaction is not started"
          end
          assert !conn2.transaction_started, "conn2 transaction is still active"
          assert conn1.transaction_started, "conn1 transaction is not still active"
        end
        assert !conn1.transaction_started, "conn1 transaction is still active"
        conn2.drop
      end
      conn1.drop
    end
  end

  def test_transaction_options_snapshot
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    sql_delete = "DELETE FROM TEST WHERE ID < ?"
    Database.create(@parms) do |conn1|
      conn1.execute(sql_schema)
      conn1.transaction do
        10.times do |i|
          conn1.execute(sql_insert, i, "NAME#{i}")
        end
      end
      Database.connect(@parms) do |conn2|
        conn2.transaction("SNAPSHOT") do
          affected = conn1.execute(sql_delete, 5)
          assert_equal 5, affected
          rs1 = conn2.query(sql_select)
          assert_equal 10, rs1.size
        end
        rs2 = conn2.query(sql_select)
        assert_equal 5, rs2.size
      end
    end
  end

  def test_transaction_options_read_committed
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    sql_delete = "DELETE FROM TEST WHERE ID < ?"
    Database.create(@parms) do |conn1|
      conn1.execute(sql_schema)
      conn1.transaction do
        10.times do |i|
          conn1.execute(sql_insert, i, "NAME#{i}")
        end
      end
      Database.connect(@parms) do |conn2|
        conn2.transaction("READ COMMITTED") do
          affected = conn1.execute(sql_delete, 5)
          assert_equal 5, affected
          rs1 = conn2.query(sql_select)
          assert_equal 5, rs1.size
        end
        rs2 = conn2.query(sql_select)
        assert_equal 5, rs2.size
      end
    end
  end

  def test_auto_and_explicit_transactions
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    sql_update = "UPDATE TEST SET NAME = 'NAME 0' WHERE ID = 10"
    Database.create(@parms) do |conn|
      conn.execute(sql_schema)
      conn.transaction { 10.times { |i| conn.execute(sql_insert, i, "NAME#{i}") } }
      result = conn.query(sql_select)
      assert !conn.transaction_started
      conn.transaction("READ COMMITTED") do
        assert conn.transaction_started
        conn.execute(sql_update)
      end
      assert !conn.transaction_started
    end
  end
end
