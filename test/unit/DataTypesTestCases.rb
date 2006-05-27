require 'test/unit'
require 'test/unit/FbTestCases'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class DataTypesTestCases < Test::Unit::TestCase
  include FbTestCases
  
  def gen_i(i)
    i
  end
  
  def gen_si(i)
    i
  end
  
  def gen_bi(i)
    i * 1000000000
  end
  
  def gen_f(i)
    i / 2
  end
  
  def gen_d(i)
    i * 3333 / 2
  end
  
  def gen_c(i)
    "%c" % (i + 64)
  end
  
  def gen_c10(i)
    gen_c(i) * 5
  end
  
  def gen_vc(i)
    gen_c(i)
  end
  
  def gen_vc10(i)
    gen_c(i) * i
  end
  
  def gen_vc10000(i)
    gen_c(i) * i * 1000
  end
  
  def gen_dt(i)
    Time.local(2000 + i)
  end
  
  def gen_tm(i)
    Time.utc(1990, 1, 1, 12, i, i)
  end
  
  def gen_ts(i)
    Time.local(2006, 1, 1, i, i, i)
  end
  
  def test_insert_basic_types
    sql_schema = <<-END
      create table TEST (
        I INTEGER,
        SI SMALLINT,
        BI BIGINT,
        F FLOAT, 
        D DOUBLE PRECISION,
        C CHAR,
        C10 CHAR(10),
        VC VARCHAR(1),
        VC10 VARCHAR(10),
        VC10000 VARCHAR(10000),
        DT DATE,
        TM TIME,
        TS TIMESTAMP);
      END
    sql_insert = <<-END
      insert into test 
        (I, SI, BI, F, D, C, C10, VC, VC10, VC10000, DT, TM, TS) 
        values
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
      END
    sql_select = "select * from TEST order by I"
    Database.create(@parms) do |connection|
      connection.execute(sql_schema);
      connection.commit;
      10.times do |i|
        connection.execute(
          sql_insert, 
          gen_i(i), gen_si(i), gen_bi(i),
          gen_f(i), gen_d(i),
          gen_c(i), gen_c10(i), gen_vc(i), gen_vc10(i), gen_vc10000(i), 
          gen_dt(i), gen_tm(i), gen_ts(i))
      end
      connection.commit
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :hash do |row|
          assert_equal gen_i(i), row["I"], "INTEGER"
          assert_equal gen_si(i), row["SI"], "SMALLINT"
          assert_equal gen_bi(i), row["BI"], "BIGINT"
          assert_equal gen_f(i), row["F"], "FLOAT"
          assert_equal gen_d(i), row["D"], "DOUBLE PRECISION"
          assert_equal gen_c(i), row["C"], "CHAR"
          assert_equal gen_c10(i).ljust(10), row["C10"], "CHAR(10)"
          assert_equal gen_vc(i), row["VC"], "VARCHAR(1)"
          assert_equal gen_vc10(i), row["VC10"], "VARCHAR(10)"
          assert_equal gen_vc10000(i), row["VC10000"], "VARCHAR(10000)"
          assert_equal gen_dt(i), row["DT"], "DATE"
          #assert_equal gen_tm(i).strftime("%H%M%S"), row["TM"].utc.strftime("%H%M%S"), "TIME"
          assert_equal gen_ts(i), row["TS"], "TIMESTAMP"
          i += 1
        end
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
end
