require 'test/FbTestCases'

class CursorTestCases < FbTestCase
  include FbTestCases

  def test_fetch_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :array
        assert_instance_of Array, row
        if @fb_version == 3
          assert_equal 5, row.size
        else
          assert_equal 4, row.size
        end
      end
      connection.execute("select * from rdb$database where rdb$description = 'bogus'") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :array
        assert_instance_of NilClass, row
        assert_nil row
      end
      connection.drop
    end
  end

  def test_fetch_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :hash
        assert_instance_of Hash, row
        if @fb_version == 3
          assert_equal 5, row.size
        else
          assert_equal 4, row.size
        end
      end
      connection.execute("select * from rdb$database where rdb$description = 'bogus'") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :hash
        assert_instance_of NilClass, row
        assert_nil row
      end
      connection.drop
    end
  end

  def test_fetch_all_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        rows = cursor.fetchall :array
        assert_instance_of Array, rows
        assert_equal 1, rows.size
        assert_instance_of Array, rows[0]
        if @fb_version == 3
          assert_equal 5, rows[0].size
        else
          assert_equal 4, rows[0].size
        end
      end
      connection.drop
    end
  end

  def test_fetch_all_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        rows = cursor.fetchall :hash
        assert_instance_of Array, rows
        assert_equal 1, rows.size
        assert_instance_of Hash, rows[0]
        if @fb_version == 3
          assert_equal 5, rows[0].size
        else
          assert_equal 4, rows[0].size
        end
      end
      connection.drop
    end
  end

  def test_fields_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields
        fields_ary = cursor.fields :array
        assert_equal fields, fields_ary
        if @fb_version == 3
          assert_equal 5, fields.size
        else
          assert_equal 4, fields.size
        end
        assert_equal "RDB$DESCRIPTION", fields[0].name;
        assert_equal "RDB$RELATION_ID", fields[1].name;
        assert_equal "RDB$SECURITY_CLASS", fields[2].name;
        assert_equal "RDB$CHARACTER_SET_NAME", fields[3].name;
        if @fb_version == 3
          assert_equal "RDB$LINGER", fields[4].name;
        end
      end
      connection.drop
    end
  end

  def test_fields_array_downcased
    Database.create(@parms.merge(:downcase_names => true)) do |connection| # xxx true
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields
        fields_ary = cursor.fields :array
        assert_equal fields, fields_ary
        if @fb_version == 3
          assert_equal 5, fields.size
        else
          assert_equal 4, fields.size
        end
        assert_equal "rdb$description", fields[0].name;
        assert_equal "rdb$relation_id", fields[1].name;
        assert_equal "rdb$security_class", fields[2].name;
        assert_equal "rdb$character_set_name", fields[3].name;
        if @fb_version == 3
          assert_equal "rdb$linger", fields[4].name;
        end
      end
      connection.drop
    end
  end

  def test_fields_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields :hash
        if @fb_version == 3
          assert_equal 5, fields.size
        else
          assert_equal 4, fields.size
        end
        assert_equal 520, fields["RDB$DESCRIPTION"].type_code
        assert_equal 500, fields["RDB$RELATION_ID"].type_code
        assert_equal 452, fields["RDB$SECURITY_CLASS"].type_code
        assert_equal 452, fields["RDB$CHARACTER_SET_NAME"].type_code
        if @fb_version == 3
          assert_equal 496, fields["RDB$LINGER"].type_code
        end
      end
      connection.drop
    end
  end

  def test_fields_hash_downcased
    Database.create(@parms.merge(:downcase_names => true)) do |connection| # xxx true
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields :hash
        if @fb_version == 3
          assert_equal 5, fields.size
        else
          assert_equal 4, fields.size
        end
        assert_equal 520, fields["rdb$description"].type_code
        assert_equal 500, fields["rdb$relation_id"].type_code
        assert_equal 452, fields["rdb$security_class"].type_code
        assert_equal 452, fields["rdb$character_set_name"].type_code
        if @fb_version == 3
          assert_equal 496, fields["rdb$linger"].type_code
        end
      end
      connection.drop
    end
  end

  def test_each_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        count = 0
        cursor.each :array do |row|
          count += 1
          assert_instance_of Array, row
          if @fb_version == 3
            assert_equal 5, row.size
          else
            assert_equal 4, row.size
          end
        end
        assert_equal 1, count
      end
      connection.drop
    end
  end

  def test_each_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        count = 0
        cursor.each :hash do |row|
          count += 1
          assert_instance_of Hash, row
          if @fb_version == 3
            assert_equal 5, row.size
          else
            assert_equal 4, row.size
          end
        end
        assert_equal 1, count
      end
      connection.drop
    end
  end

  def test_fetch_after_nil
    Database.create(@parms) do |connection|
      connection.execute("create generator test_seq");
      connection.execute("select gen_id(test_seq, 1) from rdb$database") do |cursor|
        r1 = cursor.fetch
        assert !r1.nil?
        r2 = cursor.fetch
        assert_nil r2
        assert_raises Error do
          r3 = cursor.fetch
        end
      end
      connection.execute("select * from rdb$database") do |cursor|
        r1 = cursor.fetch
        assert !r1.nil?
        r2 = cursor.fetch
        assert_nil r2
        assert_raises Error do
          r3 = cursor.fetch
        end
      end
      connection.drop
    end
  end

  def test_fetch_hash_with_aliased_fields
    sql = "SELECT RDB$DESCRIPTION DES, RDB$RELATION_ID REL, RDB$SECURITY_CLASS SEC, RDB$CHARACTER_SET_NAME FROM RDB$DATABASE"
    Database.create(@parms) do |connection|
      connection.execute(sql) do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :hash
        assert_instance_of Hash, row
        assert_equal 4, row.size
        assert row.keys.include?("DES"), "No field DES"
        assert row.keys.include?("REL"), "No field REL"
        assert row.keys.include?("SEC"), "No field SEC"
        assert row.keys.include?("RDB$CHARACTER_SET_NAME"), "No field RDB$CHARACTER_SET_NAME"
      end
      connection.drop
    end
  end
  
  def test_simultaneous_cursors
    sql_schema = <<-END
      CREATE TABLE MASTER (ID INT, NAME1 VARCHAR(10));
      CREATE TABLE DETAIL (ID INT, MASTER_ID INT, NAME2 VARCHAR(10));
    END
    sql_insert_master = "INSERT INTO MASTER (ID, NAME1) VALUES (?, ?)"
    sql_insert_detail = "INSERT INTO DETAIL (ID, MASTER_ID, NAME2) VALUES (?, ?, ?)"
    sql_select_master = "SELECT * FROM MASTER ORDER BY ID"
    sql_select_detail = "SELECT * FROM DETAIL ORDER BY ID"
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      connection.transaction do
        3.times do |m|
          connection.execute(sql_insert_master, m, "name_#{m}")
        end
        9.times do |d|
          connection.execute(sql_insert_detail, d, d / 3, "name_#{d / 3}_#{d}")
        end
      end
      master = connection.execute(sql_select_master)
      begin
        detail = connection.execute(sql_select_detail)
        begin
          3.times do |m|
            mr = master.fetch
            assert_equal m, mr[0]
            assert_equal "name_#{m}", mr[1]
            3.times do |d|
              dr = detail.fetch
              assert_equal m * 3 + d, dr[0]
              assert_equal m, dr[1]
              assert_equal "name_#{m}_#{m * 3 + d}", dr[2]
            end
          end
        ensure
          detail.close
        end
      ensure
        master.close
      end
    end
  end
end
