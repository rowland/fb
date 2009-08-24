require 'test/unit'
require 'test/FbTestCases'
# require 'fb'
# include Fb

class ConnectionTestCases < Test::Unit::TestCase
  include FbTestCases
  
  def test_execute
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_select = "SELECT * FROM RDB$DATABASE"
    Database.create(@parms) do |connection|
      assert !connection.transaction_started
      connection.execute(sql_schema)
      connection.execute(sql_select)
      assert connection.transaction_started
      connection.commit
      assert !connection.transaction_started
      connection.drop
    end
  end
  
  def test_query_select
    sql_select = "SELECT * FROM RDB$DATABASE"
    Database.create(@parms) do |connection|
      d = connection.query(sql_select)
      assert_instance_of Array, d
      assert_equal 1, d.size
      assert_instance_of Array, d.first
      assert_equal 4, d.first.size

      a = connection.query(:array, sql_select)
      assert_instance_of Array, a
      assert_equal 1, a.size
      assert_instance_of Array, a.first
      assert_equal 4, a.first.size

      h = connection.query(:hash, sql_select)
      assert_instance_of Array, h
      assert_equal 1, h.size
      assert_instance_of Hash, h.first
      assert_equal 4, h.first.keys.size
      assert h.first.keys.include?("RDB$DESCRIPTION")
      assert h.first.keys.include?("RDB$RELATION_ID")
      assert h.first.keys.include?("RDB$SECURITY_CLASS")
      assert h.first.keys.include?("RDB$CHARACTER_SET_NAME")
    end
  end
  
  def test_query_update
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_update = "UPDATE TEST SET ID = ?, NAME = ? WHERE ID = ?"
    sql_delete = "DELETE FROM TEST WHERE ID = ?"
    sql_select = "SELECT * FROM TEST"
    Database.create(@parms) do |connection|
      su = connection.query(sql_schema)
      assert_equal -1, su
      
      i = connection.query(sql_insert, 1, "NAME")
      assert_equal 1, i
      
      u = connection.query(sql_update, 1, "NAME2", 1)
      assert_equal 1, u
      
      d = connection.query(sql_delete, 1)
      assert_equal 1, d
      
      q = connection.query(sql_select)
      assert_instance_of Array, q
      assert_equal 0, q.size
    end
  end
  
  def test_insert_blobs_text
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20), MEMO BLOB SUB_TYPE TEXT)"
    sql_insert = "INSERT INTO TEST (ID, NAME, MEMO) VALUES (?, ?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      memo = IO.read("fb.c")
      assert memo.size > 50000
      connection.transaction do
        10.times do |i|
          connection.execute(sql_insert, i, i.to_s, memo);
        end
      end
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
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20), ATTACHMENT BLOB SEGMENT SIZE 1000)"
    sql_insert = "INSERT INTO TEST (ID, NAME, ATTACHMENT) VALUES (?, ?, ?)"
    sql_select = "SELECT * FROM TEST ORDER BY ID"
    filename = "fb.c"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      attachment = File.open(filename,"rb") do |f|
        f.read * 3
      end
      assert (attachment.size > 150000), "Not expected size"
      connection.transaction do
        3.times do |i|
          connection.execute(sql_insert, i, i.to_s, attachment);
        end
      end
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

  def test_rows_affected
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_update = "UPDATE TEST SET NAME = 'no name' WHERE ID < ?"
    sql_delete = "DELETE FROM TEST WHERE ID > ?"
    sql_select = "SELECT * FROM TEST"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.transaction do
        10.times do |i|
          affected = connection.execute(sql_insert, i, "name");
          assert_equal 1, affected
        end
      end
      affected = connection.execute(sql_update, 5)
      assert_equal 5, affected
      affected = connection.execute(sql_delete, 5)
      assert 4, affected
      rows = connection.execute(sql_select) do |cursor| cursor.fetchall end
      assert 6, rows.size
    end
  end

  def test_multi_insert
    sql_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
    sql_insert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
    sql_select = "SELECT * FROM TEST"
    sql_data = [
      [1, "Name 1"],
      [2, "Name 2"],
      [3, "Name 3"]]
    sql_data1 = [4, "Name 4"]
    sql_data2 = [5, "Name 5"]
    sql_data3 = [6, "Name 6"]
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.execute(sql_insert, sql_data)
      rs = connection.query(sql_select)
      assert_equal 3, rs.size

      connection.execute(sql_insert, sql_data1, sql_data2, sql_data3)
      rs = connection.query(sql_select)
      assert_equal 6, rs.size
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
    sql_schema = <<-END
      CREATE TABLE TEST1 (ID INT);
      CREATE TABLE TEST2 (ID INT);
    END
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      table_names = connection.table_names
      assert_equal 'TEST1', table_names[0]
      assert_equal 'TEST2', table_names[1]
    end
  end

  def test_table_names_downcased
    sql_schema = <<-END
      CREATE TABLE TEST1 (ID INT);
      CREATE TABLE "Test2" (ID INT);
    END
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute_script(sql_schema)
      table_names = connection.table_names
      assert_equal 'test1', table_names[0]
      assert_equal 'Test2', table_names[1]
    end
  end

  def test_generator_names
    sql_schema = <<-END
      CREATE GENERATOR TEST1_SEQ;
      CREATE GENERATOR TEST2_SEQ;
    END
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      names = connection.generator_names
      assert_equal 'TEST1_SEQ', names[0]
      assert_equal 'TEST2_SEQ', names[1]
    end
  end

  def test_generator_names_downcased
    sql_schema = <<-END
      CREATE GENERATOR TEST1_SEQ;
      CREATE GENERATOR "TEST2_seq";
    END
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute_script(sql_schema)
      names = connection.generator_names
      assert_equal 'test1_seq', names[0]
      assert_equal 'TEST2_seq', names[1]
    end
  end

  def test_view_names
    sql_schema = <<-END
      CREATE TABLE TEST1 (ID INT, NAME1 VARCHAR(10));
      CREATE TABLE TEST2 (ID INT, NAME2 VARCHAR(10));
      CREATE VIEW VIEW1 AS SELECT TEST1.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.ID = TEST2.ID;
      CREATE VIEW VIEW2 AS SELECT TEST2.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.NAME1 = TEST2.NAME2;
    END
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      names = connection.view_names
      assert_equal 'VIEW1', names[0]
      assert_equal 'VIEW2', names[1]
    end
  end

  def test_view_names_downcased
    sql_schema = <<-END
      CREATE TABLE TEST1 (ID INT, NAME1 VARCHAR(10));
      CREATE TABLE TEST2 (ID INT, NAME2 VARCHAR(10));
      CREATE VIEW VIEW1 AS SELECT TEST1.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.ID = TEST2.ID;
      CREATE VIEW "View2" AS SELECT TEST2.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.NAME1 = TEST2.NAME2;
    END
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute_script(sql_schema)
      names = connection.view_names
      assert_equal 'view1', names[0]
      assert_equal 'View2', names[1]
    end
  end

  def test_role_names
    sql_schema = <<-END
      create role reader;
      create role writer;
    END
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      names = connection.role_names
      assert_equal 'READER', names[0]
      assert_equal 'WRITER', names[1]
    end
  end
  
  def test_role_names_downcased
    sql_schema = <<-END
      create role reader;
      create role writer;
    END
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute_script(sql_schema)
      names = connection.role_names
      assert_equal 'reader', names[0]
      assert_equal 'writer', names[1]
    end
  end
  
  def test_procedure_names
    sql_schema = <<-END_SQL
      CREATE PROCEDURE PLUSONE(NUM1 INTEGER) RETURNS (NUM2 INTEGER) AS
      BEGIN
        NUM2 = NUM1 + 1;
        SUSPEND;
      END;
    END_SQL
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      names = connection.procedure_names
      assert_equal 'PLUSONE', names[0]
    end
  end

  def test_procedure_names_downcased
    sql_schema = <<-END_SQL
      CREATE PROCEDURE PLUSONE(NUM1 INTEGER) RETURNS (NUM2 INTEGER) AS
      BEGIN
        NUM2 = NUM1 + 1;
        SUSPEND;
      END;
    END_SQL
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute(sql_schema)
      names = connection.procedure_names
      assert_equal 'plusone', names[0]
    end
  end
  
  def test_trigger_names
    table_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20)); CREATE GENERATOR TEST_SEQ;"
    trigger_schema = <<-END_SQL
      CREATE TRIGGER TEST_INSERT FOR TEST ACTIVE BEFORE INSERT AS
      BEGIN
        IF (NEW.ID IS NULL) THEN
          NEW.ID = CAST(GEN_ID(TEST_SEQ, 1) AS INT);
      END
    END_SQL
    Database.create(@parms) do |connection|
      table_schema.split(';').each do |sql|
        connection.execute(sql)
      end
      connection.execute(trigger_schema)
      names = connection.trigger_names
      assert names.include?('TEST_INSERT')
    end
  end

  def test_trigger_names_downcased
    table_schema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20)); CREATE GENERATOR TEST_SEQ;"
    trigger_schema = <<-END_SQL
      CREATE TRIGGER TEST_INSERT FOR TEST ACTIVE BEFORE INSERT AS
      BEGIN
        IF (NEW.ID IS NULL) THEN
          NEW.ID = CAST(GEN_ID(TEST_SEQ, 1) AS INT);
      END
    END_SQL
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      table_schema.split(';').each do |sql|
        connection.execute(sql)
      end
      connection.execute(trigger_schema)
      names = connection.trigger_names
      assert names.include?('test_insert')
    end
  end

  def test_index_names
    sql_schema = <<-END
      CREATE TABLE MASTER (ID INT NOT NULL, NAME1 VARCHAR(10));
      CREATE TABLE DETAIL (ID INT NOT NULL, MASTER_ID INT, NAME2 VARCHAR(10));
      ALTER TABLE MASTER ADD CONSTRAINT PK_MASTER PRIMARY KEY(ID);
      ALTER TABLE DETAIL ADD CONSTRAINT PK_DETAIL PRIMARY KEY(ID);
      ALTER TABLE DETAIL ADD CONSTRAINT FK_DETAIL_MASTER_ID FOREIGN KEY(MASTER_ID) REFERENCES MASTER(ID);
      CREATE UNIQUE ASCENDING INDEX IX_MASTER_NAME1 ON MASTER(NAME1);
      CREATE DESCENDING INDEX IX_DETAIL_ID_DESC ON DETAIL(ID);
    END
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      indexes = connection.indexes # Hash of Structs using index names as keys
      assert_equal 5, indexes.size
      assert indexes.keys.include?('PK_MASTER')
      assert indexes.keys.include?('PK_DETAIL')
      assert indexes.keys.include?('FK_DETAIL_MASTER_ID')
      assert indexes.keys.include?('IX_MASTER_NAME1')
      assert indexes.keys.include?('IX_DETAIL_ID_DESC')
      
      assert indexes['PK_MASTER'].columns.include?('ID')
      assert indexes['PK_DETAIL'].columns.include?('ID')

      master_indexes = indexes.values.select {|ix| ix.table_name == 'MASTER' }
      assert_equal 2, master_indexes.size
      
      detail_indexes = indexes.values.select {|ix| ix.table_name == 'DETAIL' }
      assert_equal 3, detail_indexes.size
      
      assert_equal 'MASTER', indexes['PK_MASTER'].table_name
      assert indexes['PK_MASTER'].unique
      assert !indexes['PK_MASTER'].descending

      assert_equal 'MASTER', indexes['IX_MASTER_NAME1'].table_name
      assert indexes['IX_MASTER_NAME1'].unique
      assert !indexes['IX_MASTER_NAME1'].descending
      
      assert_equal 'DETAIL', indexes['PK_DETAIL'].table_name
      assert indexes['PK_DETAIL'].unique
      assert !indexes['PK_DETAIL'].descending

      assert_equal 'DETAIL', indexes['FK_DETAIL_MASTER_ID'].table_name
      assert !indexes['FK_DETAIL_MASTER_ID'].unique
      assert !indexes['FK_DETAIL_MASTER_ID'].descending

      assert_equal 'DETAIL', indexes['IX_DETAIL_ID_DESC'].table_name
      assert !indexes['IX_DETAIL_ID_DESC'].unique
      assert indexes['IX_DETAIL_ID_DESC'].descending      
      
      connection.drop
    end
  end

  def test_index_names_downcased
    sql_schema = <<-END
      CREATE TABLE MASTER (ID INT NOT NULL, NAME1 VARCHAR(10));
      CREATE TABLE DETAIL (ID INT NOT NULL, MASTER_ID INT, NAME2 VARCHAR(10));
      ALTER TABLE MASTER ADD CONSTRAINT PK_MASTER PRIMARY KEY(ID);
      ALTER TABLE DETAIL ADD CONSTRAINT PK_DETAIL PRIMARY KEY(ID);
      ALTER TABLE DETAIL ADD CONSTRAINT FK_DETAIL_MASTER_ID FOREIGN KEY(MASTER_ID) REFERENCES MASTER(ID);
      CREATE UNIQUE ASCENDING INDEX IX_MASTER_NAME1 ON MASTER(NAME1);
      CREATE DESCENDING INDEX "IX_DETAIL_ID_desc" ON DETAIL(ID);
    END
    Database.create(@parms.merge(:downcase_names => true)) do |connection|
      connection.execute_script(sql_schema)
      indexes = connection.indexes # Hash of Structs using index names as keys
      assert_equal 5, indexes.size
      assert indexes.keys.include?('pk_master')
      assert indexes.keys.include?('pk_detail')
      assert indexes.keys.include?('fk_detail_master_id')
      assert indexes.keys.include?('ix_master_name1')
      assert indexes.keys.include?('IX_DETAIL_ID_desc')
      assert indexes['pk_master'].columns.include?('id'), "columns missing id"
      assert indexes['pk_detail'].columns.include?('id'), "columns missing id"
      connection.drop
    end
  end
end
