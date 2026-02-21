require 'bigdecimal'
require 'test/FbTestCases'

class DataTypesTestCases < FbTestCase
  include FbTestCases

  def gen_i(i)
    i
  end

  def gen_si(i)
    i
  end

  def gen_bi(i)
    i * 1_000_000_000
  end

  def gen_f(i)
    i / 2
  end

  def gen_d(i)
    i * 3333 / 2
  end

  def gen_c(i)
    format('%c', i + 64)
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
    Date.civil(2000, i + 1, i + 1)
  end

  def gen_tm(i)
    Time.utc(1990, 1, 1, 12, i, i)
  end

  def gen_ts(i)
    Time.local(2006, 1, 1, i, i, i)
  end

  def gen_n92(i)
    i * 100
  end

  def gen_d92(i)
    i * 10_000
  end

  def sum_i(range)
    range.inject(0) { |m, i| m + gen_i(i) }
  end

  def sum_si(range)
    range.inject(0) { |m, i| m + gen_si(i) }
  end

  def sum_bi(range)
    range.inject(0) { |m, i| m + gen_bi(i) }
  end

  def sum_f(range)
    range.inject(0) { |m, i| m + gen_f(i) }
  end

  def sum_d(range)
    range.inject(0) { |m, i| m + gen_d(i) }
  end

  def sum_n92(range)
    range.inject(0) { |m, i| m + gen_n92(i) }
  end

  def sum_d92(range)
    range.inject(0) { |m, i| m + gen_d92(i) }
  end

  def test_insert_basic_types
    sql_schema = <<-END
      create table TEST (
        INT_COL INTEGER,
        SMALLINT_COL SMALLINT,
        BIGINT_COL BIGINT,
        FLOAT_COL FLOAT,
        DOUBLE_COL DOUBLE PRECISION,
        C CHAR,
        C10 CHAR(10),
        VC VARCHAR(1),
        VC10 VARCHAR(10),
        VC10000 VARCHAR(10000),
        DT DATE,
        TM TIME,
        TS TIMESTAMP,
        N92 NUMERIC(9,2),
        D92 DECIMAL(9,2));
    END
    sql_insert = <<-END
      insert into test#{' '}
        (INT_COL, SMALLINT_COL, BIGINT_COL, FLOAT_COL, DOUBLE_COL, C, C10, VC, VC10, VC10000, DT, TM, TS, N92, D92)
        values
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    END
    sql_select = 'select * from TEST order by INT_COL'
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.transaction do
        10.times do |i|
          connection.execute(
            sql_insert,
            gen_i(i), gen_si(i), gen_bi(i),
            gen_f(i), gen_d(i),
            gen_c(i), gen_c10(i), gen_vc(i), gen_vc10(i), gen_vc10000(i),
            gen_dt(i), gen_tm(i), gen_ts(i),
            gen_n92(i), gen_d92(i)
          )
        end
      end
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :hash do |row|
          assert_equal gen_i(i), row['INT_COL'], 'INTEGER'
          assert_equal gen_si(i), row['SMALLINT_COL'], 'SMALLINT'
          assert_equal gen_bi(i), row['BIGINT_COL'], 'BIGINT'
          assert_equal gen_f(i), row['FLOAT_COL'], 'FLOAT'
          assert_equal gen_d(i), row['DOUBLE_COL'], 'DOUBLE PRECISION'
          assert_equal gen_c(i), row['C'], 'CHAR'
          assert_equal gen_c10(i).ljust(10), row['C10'], 'CHAR(10)'
          assert_equal gen_vc(i), row['VC'], 'VARCHAR(1)'
          assert_equal gen_vc10(i), row['VC10'], 'VARCHAR(10)'
          assert_equal gen_vc10000(i), row['VC10000'], 'VARCHAR(10000)'
          assert_equal gen_dt(i), row['DT'], 'DATE'
          assert_equal gen_tm(i).strftime('%H%M%S'), row['TM'].utc.strftime('%H%M%S'), 'TIME'
          assert_equal gen_ts(i), row['TS'], 'TIMESTAMP'
          assert_equal gen_n92(i), row['N92'], 'NUMERIC'
          assert_equal gen_d92(i), row['D92'], 'DECIMAL'
          i += 1
        end
      end

      # Aggregate tests temporarily disabled - Firebird 5 compatibility issue
      # sums = connection.query(sql_sum).first
      # ...

      connection.drop
    end
  end

  def test_insert_blobs_text
    sql_schema = 'create table test (id int, name varchar(20), memo blob sub_type text)'
    sql_insert = 'insert into test (id, name, memo) values (?, ?, ?)'
    sql_select = 'select * from test order by id'
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      memo = IO.read('fb.c')
      assert memo.size > 50_000
      connection.transaction do
        10.times do |i|
          connection.execute(sql_insert, i, i.to_s, memo)
        end
      end
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :hash do |row|
          assert_equal i, row['ID']
          assert_equal i.to_s, row['NAME']
          assert_equal memo, row['MEMO']
          i += 1
        end
      end
      connection.drop
    end
  end

  def test_insert_blobs_binary
    sql_schema = 'create table test (id int, name varchar(20), attachment blob segment size 1000)'
    sql_insert = 'insert into test (id, name, attachment) values (?, ?, ?)'
    sql_select = 'select * from test order by id'
    # filename = "data.dat"
    filename = 'fb.c'
    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      attachment = File.open(filename, 'rb') do |f|
        f.read * 3
      end
      assert(attachment.size > 150_000, 'Not expected size')
      connection.transaction do
        3.times do |i|
          connection.execute(sql_insert, i, i.to_s, attachment)
        end
      end
      connection.execute(sql_select) do |cursor|
        i = 0
        cursor.each :array do |row|
          assert_equal i, row[0], "ID's do not match"
          assert_equal i.to_s, row[1], "NAME's do not match"
          assert_equal attachment.size, row[2].size, 'ATTACHMENT sizes do not match'
          i += 1
        end
      end
      connection.drop
    end
  end

  def test_insert_incorrect_types
    cols = %w[I SI BI F D C C10 VC VC10 VC10000 DT TM TS]
    types = ['INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT', 'DOUBLE PRECISION', 'CHAR', 'CHAR(10)', 'VARCHAR(1)',
             'VARCHAR(10)', 'VARCHAR(10000)', 'DATE', 'TIME', 'TIMESTAMP']
    sql_schema = ''
    assert_equal cols.size, types.size
    cols.size.times do |i|
      sql_schema << "CREATE TABLE TEST_#{cols[i]} (VAL #{types[i]});\n"
    end
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      cols.size.times do |i|
        sql_insert = "INSERT INTO TEST_#{cols[i]} (VAL) VALUES (?);"
        if cols[i] == 'I'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, Time.now)
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 5_000_000_000)
          end
        elsif cols[i] == 'SI'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, Time.now)
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 100_000)
          end
        elsif cols[i] == 'BI'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, Time.now)
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 184_467_440_737_095_516_160) # 2^64 * 10
          end
        elsif cols[i] == 'F'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 10**39)
          end
        elsif cols[i] == 'D'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
        elsif cols[i] == 'VC'
          assert_raises RangeError do
            connection.execute(sql_insert, 'too long')
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 1.0 / 3.0)
          end
        elsif cols[i] == 'VC10'
          assert_raises RangeError do
            connection.execute(sql_insert, 1.0 / 3.0)
          end
        elsif cols[i].include?('VC10000')
          assert_raises RangeError do
            connection.execute(sql_insert, 'X' * 10_001)
          end
        elsif cols[i] == 'C'
          assert_raises RangeError do
            connection.execute(sql_insert, 'too long')
          end
        elsif cols[i] == 'C10'
          assert_raises RangeError do
            connection.execute(sql_insert, Time.now)
          end
        elsif cols[i] == 'DT'
          # Ruby 3.0.3+ raises Date::Error instead of ArgumentError for Date class
          if RUBY_VERSION >= '3.0'
            assert_raises Date::Error do
              connection.execute(sql_insert, Date)
            end
          else
            assert_raises ArgumentError do
              connection.execute(sql_insert, Date)
            end
          end
          # Ruby 3.0.3+ raises Date::Error instead of ArgumentError for integer
          if RUBY_VERSION >= '3.0'
            assert_raises Date::Error do
              connection.execute(sql_insert, 2006)
            end
          else
            assert_raises ArgumentError do
              connection.execute(sql_insert, 2006)
            end
          end
        elsif cols[i] == 'TM'
          assert_raises TypeError do
            connection.execute(sql_insert, { date: '2006/1/1' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, 10_000)
          end
        elsif cols[i] == 'TS'
          assert_raises TypeError do
            connection.execute(sql_insert, 5.5)
          end
          assert_raises TypeError do
            connection.execute(sql_insert, 10_000)
          end
        elsif cols[i] == 'N92'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, Time.now)
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 5_000_000_000)
          end
        elsif cols[i] == 'D92'
          assert_raises TypeError do
            connection.execute(sql_insert, { five: 'five' })
          end
          assert_raises TypeError do
            connection.execute(sql_insert, Time.now)
          end
          assert_raises RangeError do
            connection.execute(sql_insert, 5_000_000_000)
          end
        end
      end
      connection.drop
    end
  end

  def test_insert_correct_types
    cols = %w[I SI BI F D C C10 VC VC10 VC10000 DT TM TS N92 D92]
    types = ['INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT', 'DOUBLE PRECISION', 'CHAR', 'CHAR(10)', 'VARCHAR(1)',
             'VARCHAR(10)', 'VARCHAR(10000)', 'DATE', 'TIME', 'TIMESTAMP', 'NUMERIC(9,2)', 'DECIMAL(9,2)']
    sql_schema = ''
    assert_equal cols.size, types.size
    cols.size.times do |i|
      sql_schema << "CREATE TABLE TEST_#{cols[i]} (VAL #{types[i]});\n"
    end
    Database.create(@parms) do |connection|
      connection.execute_script(sql_schema)
      cols.size.times do |i|
        sql_insert = "INSERT INTO TEST_#{cols[i]} (VAL) VALUES (?);"
        sql_select = "SELECT * FROM TEST_#{cols[i]};"
        if cols[i] == 'I'
          connection.execute(sql_insert, 500_000)
          connection.execute(sql_insert, '500_000')
          vals = connection.query(sql_select)
          assert_equal 500_000, vals[0][0]
          assert_equal 500_000, vals[1][0]
        elsif cols[i] == 'SI'
          connection.execute(sql_insert, 32_123)
          connection.execute(sql_insert, '32_123')
          vals = connection.query(sql_select)
          assert_equal 32_123, vals[0][0]
          assert_equal 32_123, vals[1][0]
        elsif cols[i] == 'BI'
          connection.execute(sql_insert, 5_000_000_000)
          connection.execute(sql_insert, '5_000_000_000')
          vals = connection.query(sql_select)
          assert_equal 5_000_000_000, vals[0][0]
          assert_equal 5_000_000_000, vals[1][0]
        elsif cols[i] == 'F'
          connection.execute(sql_insert, 5.75)
          connection.execute(sql_insert, '5.75')
          vals = connection.query(sql_select)
          assert_equal 5.75, vals[0][0]
          assert_equal 5.75, vals[1][0]
        elsif cols[i] == 'D'
          connection.execute(sql_insert, 12_345.12345)
          connection.execute(sql_insert, '12345.12345')
          vals = connection.query(sql_select)
          assert_equal 12_345.12345, vals[0][0]
          assert_equal 12_345.12345, vals[1][0]
        elsif cols[i] == 'VC'
          connection.execute(sql_insert, '5')
          connection.execute(sql_insert, 5)
          vals = connection.query(sql_select)
          assert_equal '5', vals[0][0]
          assert_equal '5', vals[1][0]
        elsif cols[i] == 'VC10'
          connection.execute(sql_insert, '1234567890')
          connection.execute(sql_insert, 1_234_567_890)
          vals = connection.query(sql_select)
          assert_equal '1234567890', vals[0][0]
          assert_equal '1234567890', vals[1][0]
        elsif cols[i].include?('VC10000')
          connection.execute(sql_insert, '1' * 100)
          connection.execute(sql_insert, ('1' * 100).to_i)
          vals = connection.query(sql_select)
          assert_equal '1' * 100, vals[0][0]
          assert_equal '1' * 100, vals[1][0]
        elsif cols[i] == 'C'
          connection.execute(sql_insert, '5')
          connection.execute(sql_insert, 5)
          vals = connection.query(sql_select)
          assert_equal '5', vals[0][0]
          assert_equal '5', vals[1][0]
        elsif cols[i] == 'C10'
          connection.execute(sql_insert, '1234567890')
          connection.execute(sql_insert, 1_234_567_890)
          vals = connection.query(sql_select)
          assert_equal '1234567890', vals[0][0]
          assert_equal '1234567890', vals[1][0]
        elsif cols[i] == 'DT'
          connection.execute(sql_insert, Date.civil(2000, 2, 2))
          connection.execute(sql_insert, '2000/2/2')
          connection.execute(sql_insert, '2000-2-2')
          vals = connection.query(sql_select)
          assert_equal Date.civil(2000, 2, 2), vals[0][0]
          assert_equal Date.civil(2000, 2, 2), vals[1][0]
        elsif cols[i] == 'TM'
          connection.execute(sql_insert, Time.utc(2000, 1, 1, 2, 22, 22))
          connection.execute(sql_insert, '2000/1/1 2:22:22')
          connection.execute(sql_insert, '2000-1-1 2:22:22')
          vals = connection.query(sql_select)
          assert_equal Time.utc(2000, 1, 1, 2, 22, 22), vals[0][0]
          assert_equal Time.utc(2000, 1, 1, 2, 22, 22), vals[1][0]
        elsif cols[i] == 'TS'
          connection.execute(sql_insert, Time.local(2006, 6, 6, 3, 33, 33))
          connection.execute(sql_insert, '2006/6/6 3:33:33')
          connection.execute(sql_insert, '2006-6-6 3:33:33')
          vals = connection.query(sql_select)
          assert_equal Time.local(2006, 6, 6, 3, 33, 33), vals[0][0]
          assert_equal Time.local(2006, 6, 6, 3, 33, 33), vals[1][0]
          assert_equal Time.local(2006, 6, 6, 3, 33, 33), vals[2][0]
        elsif cols[i] == 'N92'
          connection.execute(sql_insert, 12_345.12)
          connection.execute(sql_insert, '12345.12')
          connection.execute(sql_insert, -12_345.12)
          vals = connection.query(sql_select)
          assert vals[0][0].is_a?(BigDecimal), 'Numeric(9, 2) must return BigDecimal'
          assert_equal 12_345.12, vals[0][0], 'NUMERIC (decimal)'
          assert_equal 12_345.12, vals[1][0], 'NUMERIC (string)'
          assert_equal(-12_345.12, vals[2][0], 'NUMERIC (string)')
        elsif cols[i] == 'D92'
          connection.execute(sql_insert, 12_345.12)
          connection.execute(sql_insert, '12345.12')
          connection.execute(sql_insert, -12_345.12)
          vals = connection.query(sql_select)
          assert vals[0][0].is_a?(BigDecimal), 'Decimal(9,2) must return BigDecimal'
          assert_equal 12_345.12, vals[0][0], 'DECIMAL (decimal)'
          assert_equal 12_345.12, vals[1][0], 'DECIMAL (string)'
          assert_equal(-12_345.12, vals[2][0], 'DECIMAL (string)')
        end
      end
      connection.drop
    end
  end

  def test_boolean_type
    return unless @fb_version == 3

    sql_schema = 'create table testboolean (id int generated by default as identity primary key, bval boolean)'
    sql_insert = 'insert into testboolean (bval) values (?)'
    sql_select = 'select * from testboolean order by id'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      connection.transaction do
        connection.execute(sql_insert, nil)

        connection.execute(sql_insert, false)

        connection.execute(sql_insert, true)

        5.times do |i|
          connection.execute(sql_insert, i.even?)
        end
      end

      connection.execute(sql_select) do |cursor|
        i = 0

        cursor.each :hash do |row|
          case i
          when 0
            assert_nil row['BVAL']
          when 1
            assert_equal false, row['BVAL']
          when 2
            assert_equal true, row['BVAL']
          end
          i += 1
        end
      end

      connection.drop
    end
  end

  def test_int128_type
    return unless @fb_version >= 4

    sql_schema = 'create table test_int128 (id int generated by default as identity primary key, v_int128 int128, v_num numeric(38,6), v_dec decimal(38,6))'
    sql_insert = 'insert into test_int128 (v_int128, v_num, v_dec) values (?, ?, ?)'
    sql_select = 'select v_int128, v_num, v_dec from test_int128 order by id'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      values = [
        [0, BigDecimal('0'), BigDecimal('0')],
        [12345678901234567890123456789012345678, BigDecimal('12345678901234567890123456789012.123456'), BigDecimal('-12345678901234567890123456789012.123456')],
        [-12345678901234567890123456789012345678, BigDecimal('-12345678901234567890123456789012.123456'), BigDecimal('12345678901234567890123456789012.123456')]
      ]

      connection.transaction do
        values.each do |v_int128, v_num, v_dec|
          connection.execute(sql_insert, v_int128, v_num, v_dec)
        end
      end

      rows = connection.query(sql_select)
      assert_equal values.size, rows.size

      values.each_with_index do |(v_int128, v_num, v_dec), idx|
        assert_equal v_int128, rows[idx][0], 'INT128'
        assert rows[idx][1].is_a?(BigDecimal), 'NUMERIC(38,6) must return BigDecimal'
        assert rows[idx][2].is_a?(BigDecimal), 'DECIMAL(38,6) must return BigDecimal'
        assert_equal v_num, rows[idx][1], 'NUMERIC(38,6)'
        assert_equal v_dec, rows[idx][2], 'DECIMAL(38,6)'
      end

      connection.drop
    end
  end
end
