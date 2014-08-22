#coding:utf-8
require 'test/FbTestCases'

class EncodingTestCases < FbTestCase
  include FbTestCases

  def test_encoding
    sql_schema = <<-END
      create table TEST (
        ID INTEGER,
        C10 CHAR(10),
        VC10 VARCHAR(10),
        MEMO BLOB SUB_TYPE TEXT)
      END
    sql_insert = <<-END
      insert into test 
        (ID, C10, VC10, MEMO) 
        values
        (?, ?, ?, ?);
      END
    sql_select = "select * from TEST order by ID"
    lorem = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."

    Database.create(@parms.update(:encoding => "UTF-8")) do |connection|
      connection.execute(sql_schema);

      connection.execute(sql_insert, 1, "abcdef", "한글", lorem)

      row = connection.query(sql_select).first
      assert_equal 1, row[0]
      assert_equal  Encoding::UTF_8, row[1].encoding
      assert_equal "abcdef    ", row[1]
      assert_equal  Encoding::UTF_8, row[2].encoding
      assert_equal "한글", row[2]
      assert_equal  Encoding::UTF_8, row[3].encoding
      assert_equal lorem, row[3]
    end
  end
end
