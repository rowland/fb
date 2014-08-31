require 'bigdecimal'
require 'test/FbTestCases'

class NumericDataTypesTestCases < FbTestCase
  include FbTestCases

  def setup
    super
    @connection = Database.create(@parms).connect
  end

  def teardown
    @connection.drop
  end

  def prepare_test_table(datatype)
    @table = "test#{@id ||= 0 + 1}"
    @connection.execute("create table #{@table} (val #{datatype})")
  end

  def write_and_read_value(insert_value)
    @connection.execute("insert into #{@table} (val) values (?)", insert_value)
    read_value = @connection.query("select * from #{@table}")[0][0]
    @connection.execute("delete from #{@table}")
    read_value
  end

  def test_smallint_max
    prepare_test_table("smallint")
    assert_equal 32767, write_and_read_value(32767)
    assert_equal 32767, write_and_read_value("32767")
    assert_equal 32767, write_and_read_value(32767.0)
    assert_equal 32767, write_and_read_value(BigDecimal("32767"))
    assert write_and_read_value(32767).is_a?(Fixnum)
    assert write_and_read_value("32767").is_a?(Fixnum)
    assert write_and_read_value(32767.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("32767")).is_a?(Fixnum)
  end

  def test_smallint_min
    prepare_test_table("smallint")
    assert_equal -32768, write_and_read_value(-32768)
    assert_equal -32768, write_and_read_value("-32768")
    assert_equal -32768, write_and_read_value(-32768.0)
    assert_equal -32768, write_and_read_value(BigDecimal("-32768"))
    assert write_and_read_value(-32768).is_a?(Fixnum)
    assert write_and_read_value("-32768").is_a?(Fixnum)
    assert write_and_read_value(-32768.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("-32768")).is_a?(Fixnum)
  end

  def test_smallint_rounding
    prepare_test_table("smallint")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_smallint_input_type
    prepare_test_table("smallint")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_smallint_input_range
    prepare_test_table("smallint")
    assert_raises(RangeError) { write_and_read_value(32768) }
    assert_raises(RangeError) { write_and_read_value("32768") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("32768")) }
    assert_raises(RangeError) { write_and_read_value(-32769) }
    assert_raises(RangeError) { write_and_read_value("-32769") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-32769")) }
  end

  def test_integer_max
    prepare_test_table("integer")
    assert_equal 2147483647, write_and_read_value(2147483647)
    assert_equal 2147483647, write_and_read_value("2147483647")
    assert_equal 2147483647, write_and_read_value(2147483647.0)
    assert_equal 2147483647, write_and_read_value(BigDecimal("2147483647"))
    assert write_and_read_value(2147483647).is_a?(Fixnum)
    assert write_and_read_value("2147483647").is_a?(Fixnum)
    assert write_and_read_value(2147483647.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("2147483647")).is_a?(Fixnum)
  end

  def test_integer_min
    prepare_test_table("integer")
    assert_equal -2147483648, write_and_read_value(-2147483648)
    assert_equal -2147483648, write_and_read_value("-2147483648")
    assert_equal -2147483648, write_and_read_value(-2147483648.0)
    assert_equal -2147483648, write_and_read_value(BigDecimal("-2147483648"))
    assert write_and_read_value(-2147483648).is_a?(Fixnum)
    assert write_and_read_value("-2147483648").is_a?(Fixnum)
    assert write_and_read_value(-2147483648.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("-2147483648")).is_a?(Fixnum)
  end

  def test_integer_rounding
    prepare_test_table("integer")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_integer_input_type
    prepare_test_table("integer")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_integer_input_range
    prepare_test_table("integer")
    assert_raises(RangeError) { write_and_read_value(2147483648) }
    assert_raises(RangeError) { write_and_read_value("2147483648") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("2147483648")) }
    assert_raises(RangeError) { write_and_read_value(-2147483649) }
    assert_raises(RangeError) { write_and_read_value("-2147483649") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-2147483649")) }
  end

  def test_bigint_max
    prepare_test_table("bigint")
    assert_equal 9223372036854775807, write_and_read_value(9223372036854775807)
    assert_equal 9223372036854775807, write_and_read_value("9223372036854775807")
    #assert_equal 9223372036854775807, write_and_read_value(9223372036854775807.0)
    assert_equal 9223372036854775807, write_and_read_value(BigDecimal("9223372036854775807"))
    assert write_and_read_value(9223372036854775807).is_a?(Bignum)
    assert write_and_read_value("9223372036854775807").is_a?(Bignum)
    #assert write_and_read_value(9223372036854775807.0).is_a?(Bignum)
    assert write_and_read_value(BigDecimal("9223372036854775807")).is_a?(Bignum)
  end

  def test_bigint_min
    prepare_test_table("bigint")
    assert_equal -9223372036854775808, write_and_read_value(-9223372036854775808)
    assert_equal -9223372036854775808, write_and_read_value("-9223372036854775808")
    #assert_equal -9223372036854775808, write_and_read_value(-9223372036854775808.0)
    assert_equal -9223372036854775808, write_and_read_value(BigDecimal("-9223372036854775808"))
    assert write_and_read_value(-9223372036854775808).is_a?(Bignum)
    assert write_and_read_value("-9223372036854775808").is_a?(Bignum)
    #assert write_and_read_value(-9223372036854775808.0).is_a?(Bignum)
    assert write_and_read_value(BigDecimal("-9223372036854775808")).is_a?(Bignum)
  end

  def test_bigint_rounding
    prepare_test_table("bigint")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_bigint_input_type
    prepare_test_table("bigint")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_bigint_input_range
    prepare_test_table("bigint")
    assert_raises(RangeError) { write_and_read_value(9223372036854775808) }
    assert_raises(RangeError) { write_and_read_value("9223372036854775808") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("9223372036854775808")) }
    assert_raises(RangeError) { write_and_read_value(-9223372036854775809) }
    assert_raises(RangeError) { write_and_read_value("-9223372036854775809") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-9223372036854775809")) }
  end

  def test_decimal_4_0_max
    prepare_test_table("decimal(4, 0)")
    assert_equal 32767, write_and_read_value(32767)
    assert_equal 32767, write_and_read_value("32767")
    assert_equal 32767, write_and_read_value(32767.0)
    assert_equal 32767, write_and_read_value(BigDecimal("32767"))
    assert write_and_read_value(32767).is_a?(Fixnum)
    assert write_and_read_value("32767").is_a?(Fixnum)
    assert write_and_read_value(32767.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("32767")).is_a?(Fixnum)
  end

  def test_decimal_4_0_min
    prepare_test_table("decimal(4, 0)")
    assert_equal -32768, write_and_read_value(-32768)
    assert_equal -32768, write_and_read_value("-32768")
    assert_equal -32768, write_and_read_value(-32768.0)
    assert_equal -32768, write_and_read_value(BigDecimal("-32768"))
    assert write_and_read_value(-32768).is_a?(Fixnum)
    assert write_and_read_value("-32768").is_a?(Fixnum)
    assert write_and_read_value(-32768.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("-32768")).is_a?(Fixnum)
  end

  def test_decimal_4_0_rounding
    prepare_test_table("decimal(4, 0)")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_decimal_4_0_input_type
    prepare_test_table("decimal(4, 0)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_4_0_input_range
    prepare_test_table("decimal(4, 0)")
    assert_raises(RangeError) { write_and_read_value(2147483648) }
    assert_raises(RangeError) { write_and_read_value("2147483648") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("2147483648")) }
    assert_raises(RangeError) { write_and_read_value(-2147483649) }
    assert_raises(RangeError) { write_and_read_value("-2147483649") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-2147483649")) }
  end

  def test_decimal_9_0_max
    prepare_test_table("decimal(9, 0)")
    assert_equal 2147483647, write_and_read_value(2147483647)
    assert_equal 2147483647, write_and_read_value("2147483647")
    assert_equal 2147483647, write_and_read_value(2147483647.0)
    assert_equal 2147483647, write_and_read_value(BigDecimal("2147483647"))
    assert write_and_read_value(2147483647).is_a?(Fixnum)
    assert write_and_read_value("2147483647").is_a?(Fixnum)
    assert write_and_read_value(2147483647.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("2147483647")).is_a?(Fixnum)
  end

  def test_decimal_9_0_min
    prepare_test_table("decimal(9, 0)")
    assert_equal 2147483647, write_and_read_value(2147483647)
    assert_equal 2147483647, write_and_read_value("2147483647")
    assert_equal 2147483647, write_and_read_value(2147483647.0)
    assert_equal 2147483647, write_and_read_value(BigDecimal("2147483647"))
    assert write_and_read_value(2147483647).is_a?(Fixnum)
    assert write_and_read_value("2147483647").is_a?(Fixnum)
    assert write_and_read_value(2147483647.0).is_a?(Fixnum)
    assert write_and_read_value(BigDecimal("2147483647")).is_a?(Fixnum)
  end

  def test_decimal_9_0_rounding
    prepare_test_table("decimal(9, 0)")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_decimal_9_0_input_type
    prepare_test_table("decimal(9, 0)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_9_0_input_range
    prepare_test_table("decimal(9, 0)")
    assert_raises(RangeError) { write_and_read_value(2147483648) }
    assert_raises(RangeError) { write_and_read_value("2147483648") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("2147483648")) }
    assert_raises(RangeError) { write_and_read_value(-2147483649) }
    assert_raises(RangeError) { write_and_read_value("-2147483649") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-2147483649")) }
  end

  def test_decimal_9_4_max
    prepare_test_table("decimal(9, 4)")
    assert_equal BigDecimal("214748.3647"), write_and_read_value("214748.3647")
    assert_equal BigDecimal("214748.3647"), write_and_read_value(214748.3647)
    assert_equal BigDecimal("214748.3647"), write_and_read_value(BigDecimal("214748.3647"))
    assert write_and_read_value("214748.3647").is_a?(BigDecimal)
    assert write_and_read_value(214748.3647).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("214748.3647")).is_a?(BigDecimal)
  end

  def test_decimal_9_4_min
    prepare_test_table("decimal(9, 4)")
    assert_equal BigDecimal("-214748.3648"), write_and_read_value("-214748.3648")
    assert_equal BigDecimal("-214748.3648"), write_and_read_value(-214748.3648)
    assert_equal BigDecimal("-214748.3648"), write_and_read_value(BigDecimal("-214748.3648"))
    assert write_and_read_value("-214748.3648").is_a?(BigDecimal)
    assert write_and_read_value(-214748.3648).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("-214748.3648")).is_a?(BigDecimal)
  end

  def test_decimal_9_4_rounding
    prepare_test_table("decimal(9, 4)")
    assert_equal 0, write_and_read_value(0.00004)
    assert_equal 0, write_and_read_value("0.00004")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.00004"))
    assert_equal 0, write_and_read_value(-0.00004)
    assert_equal 0, write_and_read_value("-0.00004")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.00004"))
    assert_equal BigDecimal('0.0001'), write_and_read_value(0.00005)
    assert_equal BigDecimal('0.0001'), write_and_read_value("0.00005")
    assert_equal BigDecimal('0.0001'), write_and_read_value(BigDecimal.new("0.00005"))
    assert_equal BigDecimal('-0.0001'), write_and_read_value(-0.00005)
    assert_equal BigDecimal('-0.0001'), write_and_read_value("-0.00005")
    assert_equal BigDecimal('-0.0001'), write_and_read_value(BigDecimal.new("-0.00005"))
  end

  def test_decimal_9_4_input_type
    prepare_test_table("decimal(9, 4)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_9_4_input_range
    prepare_test_table("decimal(9, 4)")
    assert_raises(RangeError) { write_and_read_value(214748.3648) }
    assert_raises(RangeError) { write_and_read_value("214748.3648") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("214748.3648")) }
    assert_raises(RangeError) { write_and_read_value(-214748.3649) }
    assert_raises(RangeError) { write_and_read_value("-214748.3649") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-214748.3649")) }
  end

  def test_decimal_9_9_max
    prepare_test_table("decimal(9, 9)")
    assert_equal BigDecimal("2.147483647"), write_and_read_value("2.147483647")
    assert_equal BigDecimal("2.147483647"), write_and_read_value(2.147483647)
    assert_equal BigDecimal("2.147483647"), write_and_read_value(BigDecimal("2.147483647"))
    assert write_and_read_value("2.147483647").is_a?(BigDecimal)
    assert write_and_read_value(2.147483647).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("2.147483647")).is_a?(BigDecimal)
  end

  def test_decimal_9_9_min
    prepare_test_table("decimal(9, 9)")
    assert_equal BigDecimal("-2.147483648"), write_and_read_value("-2.147483648")
    assert_equal BigDecimal("-2.147483648"), write_and_read_value(-2.147483648)
    assert_equal BigDecimal("-2.147483648"), write_and_read_value(BigDecimal("-2.147483648"))
    assert write_and_read_value("-2.147483648").is_a?(BigDecimal)
    assert write_and_read_value(-2.147483648).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("-2.147483648")).is_a?(BigDecimal)
  end

  def test_decimal_9_9_rounding
    prepare_test_table("decimal(9, 9)")
    assert_equal 0, write_and_read_value(0.0000000004)
    assert_equal 0, write_and_read_value("0.0000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.0000000004"))
    assert_equal 0, write_and_read_value(-0.0000000004)
    assert_equal 0, write_and_read_value("-0.0000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.0000000004"))
    assert_equal BigDecimal("0.000000001"), write_and_read_value(0.0000000005)
    assert_equal BigDecimal("0.000000001"), write_and_read_value("0.0000000005")
    assert_equal BigDecimal("0.000000001"), write_and_read_value(BigDecimal.new("0.0000000005"))
    assert_equal BigDecimal("-0.000000001"), write_and_read_value(-0.0000000005)
    assert_equal BigDecimal("-0.000000001"), write_and_read_value("-0.0000000005")
    assert_equal BigDecimal("-0.000000001"), write_and_read_value(BigDecimal.new("-0.0000000005"))
  end

  def test_decimal_9_9_input_type
    prepare_test_table("decimal(9, 9)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_9_9_input_range
    prepare_test_table("decimal(9, 9)")
    assert_raises(RangeError) { write_and_read_value(2.147483648) }
    assert_raises(RangeError) { write_and_read_value("2.147483648") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("2.147483648")) }
    assert_raises(RangeError) { write_and_read_value(-2.147483649) }
    assert_raises(RangeError) { write_and_read_value("-2.147483649") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-2.147483649")) }
  end

  def test_decimal_18_0_max
    prepare_test_table("decimal(18, 0)")
    assert_equal 9223372036854775807, write_and_read_value(9223372036854775807)
    assert_equal 9223372036854775807, write_and_read_value("9223372036854775807")
    #assert_equal 9223372036854775807, write_and_read_value(9223372036854775807.0)
    assert_equal 9223372036854775807, write_and_read_value(BigDecimal("9223372036854775807"))
    assert write_and_read_value(9223372036854775807).is_a?(Bignum)
    assert write_and_read_value("9223372036854775807").is_a?(Bignum)
    #assert write_and_read_value(9223372036854775807.0).is_a?(Bignum)
    assert write_and_read_value(BigDecimal("9223372036854775807")).is_a?(Bignum)
  end

  def test_decimal_18_0_min
    prepare_test_table("decimal(18, 0)")
    assert_equal -9223372036854775808, write_and_read_value(-9223372036854775808)
    assert_equal -9223372036854775808, write_and_read_value("-9223372036854775808")
    #assert_equal -9223372036854775808, write_and_read_value(-9223372036854775808.0)
    assert_equal -9223372036854775808, write_and_read_value(BigDecimal("-9223372036854775808"))
    assert write_and_read_value(-9223372036854775808).is_a?(Bignum)
    assert write_and_read_value("-9223372036854775808").is_a?(Bignum)
    #assert write_and_read_value(-9223372036854775808.0).is_a?(Bignum)
    assert write_and_read_value(BigDecimal("-9223372036854775808")).is_a?(Bignum)
  end

  def test_decimal_18_0_rounding
    prepare_test_table("decimal(18, 0)")
    assert_equal 0, write_and_read_value(0.4)
    assert_equal 0, write_and_read_value("0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.4"))
    assert_equal 0, write_and_read_value(-0.4)
    assert_equal 0, write_and_read_value("-0.4")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.4"))
    assert_equal 1, write_and_read_value(0.5)
    assert_equal 1, write_and_read_value("0.5")
    assert_equal 1, write_and_read_value(BigDecimal.new("0.5"))
    assert_equal -1, write_and_read_value(-0.5)
    assert_equal -1, write_and_read_value("-0.5")
    assert_equal -1, write_and_read_value(BigDecimal.new("-0.5"))
  end

  def test_decimal_18_0_input_types
    prepare_test_table("decimal(18, 0)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_18_0_input_range
    prepare_test_table("decimal(18, 0)")
    assert_raises(RangeError) { write_and_read_value(9223372036854775808) }
    assert_raises(RangeError) { write_and_read_value("9223372036854775808") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("9223372036854775808")) }
    assert_raises(RangeError) { write_and_read_value(-9223372036854775809) }
    assert_raises(RangeError) { write_and_read_value("-9223372036854775809") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-9223372036854775809")) }
  end

  def test_decimal_18_9_max
    prepare_test_table("decimal(18, 9)")
    assert_equal BigDecimal("9223372036.854775807"), write_and_read_value("9223372036.854775807")
    #assert_equal BigDecimal("9223372036.854775807"), write_and_read_value(9223372036.854775807)
    assert_equal BigDecimal("9223372036.854775807"), write_and_read_value(BigDecimal("9223372036.854775807"))
    assert write_and_read_value("9223372036.854775807").is_a?(BigDecimal)
    #assert write_and_read_value(9223372036.854775807).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("9223372036.854775807")).is_a?(BigDecimal)
  end

  def test_decimal_18_9_min
    prepare_test_table("decimal(18, 9)")
    assert_equal BigDecimal("-9223372036.854775808"), write_and_read_value("-9223372036.854775808")
    #assert_equal BigDecimal("-9223372036.854775808"), write_and_read_value(-9223372036.854775808)
    assert_equal BigDecimal("-9223372036.854775808"), write_and_read_value(BigDecimal("-9223372036.854775808"))
    assert write_and_read_value("-9223372036.854775808").is_a?(BigDecimal)
    #assert write_and_read_value(-9223372036.854775808).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("-9223372036.854775808")).is_a?(BigDecimal)
  end

  def test_decimal_18_9_rounding
    prepare_test_table("decimal(18, 9)")
    assert_equal 0, write_and_read_value(0.0000000004)
    assert_equal 0, write_and_read_value("0.0000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.0000000004"))
    assert_equal 0, write_and_read_value(-0.0000000004)
    assert_equal 0, write_and_read_value("-0.0000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.0000000004"))
    assert_equal BigDecimal('0.000000001'), write_and_read_value(0.0000000005)
    assert_equal BigDecimal('0.000000001'), write_and_read_value("0.0000000005")
    assert_equal BigDecimal('0.000000001'), write_and_read_value(BigDecimal.new("0.0000000005"))
    assert_equal BigDecimal('-0.000000001'), write_and_read_value(-0.0000000005)
    assert_equal BigDecimal('-0.000000001'), write_and_read_value("-0.0000000005")
    assert_equal BigDecimal('-0.000000001'), write_and_read_value(BigDecimal.new("-0.0000000005"))
  end

  def test_decimal_18_9_input_type
    prepare_test_table("decimal(18, 9)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_18_9_input_range
    prepare_test_table("decimal(18, 9)")
    assert_raises(RangeError) { write_and_read_value(9223372036.854775808) }
    assert_raises(RangeError) { write_and_read_value("9223372036.854775808") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("9223372036.854775808")) }
    assert_raises(RangeError) { write_and_read_value(-9223372036.854775809) }
    assert_raises(RangeError) { write_and_read_value("-9223372036.854775809") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-9223372036.854775809")) }
  end

  def test_decimal_18_18_max
    prepare_test_table("decimal(18, 18)")
    assert_equal BigDecimal("9.223372036854775807"), write_and_read_value(BigDecimal("9.223372036854775807"))
    #assert_equal BigDecimal("9.223372036854775807"), write_and_read_value(9.223372036854775807)
    assert_equal BigDecimal("9.223372036854775807"), write_and_read_value("9.223372036854775807")
    assert write_and_read_value("9.223372036854775807").is_a?(BigDecimal)
    #assert write_and_read_value(9.223372036854775807).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("9.223372036854775807")).is_a?(BigDecimal)
  end

  def test_decimal_18_18_min
    prepare_test_table("decimal(18, 18)")
    assert_equal BigDecimal("-9.223372036854775808"), write_and_read_value("-9.223372036854775808")
    #assert_equal BigDecimal("-9.223372036854775808"), write_and_read_value(-9.223372036854775808)
    assert_equal BigDecimal("-9.223372036854775808"), write_and_read_value(BigDecimal("-9.223372036854775808"))
    assert write_and_read_value("-9.223372036854775808").is_a?(BigDecimal)
    #assert write_and_read_value(-9.223372036854775808).is_a?(BigDecimal)
    assert write_and_read_value(BigDecimal("-9.223372036854775808")).is_a?(BigDecimal)
  end

  def test_decimal_18_18_rounding
    prepare_test_table("decimal(18, 18)")
    assert_equal 0, write_and_read_value(0.0000000000000000004)
    assert_equal 0, write_and_read_value("0.0000000000000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("0.0000000000000000004"))
    assert_equal 0, write_and_read_value(-0.0000000000000000004)
    assert_equal 0, write_and_read_value("-0.0000000000000000004")
    assert_equal 0, write_and_read_value(BigDecimal.new("-0.0000000000000000004"))
    assert_equal BigDecimal("0.000000000000000001"), write_and_read_value(0.0000000000000000005)
    assert_equal BigDecimal("0.000000000000000001"), write_and_read_value("0.0000000000000000005")
    assert_equal BigDecimal("0.000000000000000001"), write_and_read_value(BigDecimal.new("0.0000000000000000005"))
    assert_equal BigDecimal("-0.000000000000000001"), write_and_read_value(-0.0000000000000000005)
    assert_equal BigDecimal("-0.000000000000000001"), write_and_read_value("-0.0000000000000000005")
    assert_equal BigDecimal("-0.000000000000000001"), write_and_read_value(BigDecimal.new("-0.0000000000000000005"))
  end

  def test_decimal_18_18_input_type
    prepare_test_table("decimal(18, 18)")
    #assert_raises(TypeError) { write_and_read_value('abcde') }
    assert_raises(TypeError) { write_and_read_value(Date.new) }
    assert_raises(TypeError) { write_and_read_value(Time.new) }
    assert_raises(TypeError) { write_and_read_value(Object.new) }
  end

  def test_decimal_18_18_input_range
    prepare_test_table("decimal(18, 18)")
    assert_raises(RangeError) { write_and_read_value(9.223372036854775808) }
    assert_raises(RangeError) { write_and_read_value("9.223372036854775808") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("9.223372036854775808")) }
    assert_raises(RangeError) { write_and_read_value(-9.223372036854775809) }
    assert_raises(RangeError) { write_and_read_value("-9.223372036854775809") }
    assert_raises(RangeError) { write_and_read_value(BigDecimal("-9.223372036854775809")) }
  end
end
