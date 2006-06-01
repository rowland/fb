require 'test/unit'
require 'test/unit/FbTestCases'
require 'fb.so'
include Fb

class TransactionTestCases < Test::Unit::TestCase
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
      assert_raise RuntimeError do
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
      assert_raise RuntimeError do
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
      assert_raise Error do
        connection.execute(sql_insert, 1, "two")
      end
      assert !connection.transaction_started, "transaction is active"
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
      assert_raise RuntimeError do
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
end
