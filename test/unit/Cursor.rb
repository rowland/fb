require 'test/unit'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class CursorTestCases < Test::Unit::TestCase
  def setup
    @db_file = 'c:/var/fbdata/testrbfb.fdb'
    @parms = {
      :database => "localhost:#{@db_file}",
      :username => 'sysdba',
      :password => 'masterkey',
      :charset => 'NONE',
      :role => 'READER' }
    rm_rf @db_file
  end
  
  def test_fetch_array
    flunk
  end
  
  def test_fetch_hash
    flunk
  end
  
  def test_fetch_all_array
    flunk
  end
  
  def test_fetch_all_hash
    flunk
  end
  
  def test_fields_array
    flunk
  end
  
  def test_fields_hash
    flunk
  end
end
