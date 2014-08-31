$: << File.dirname(__FILE__)
$: << File.join(File.dirname(__FILE__),'..')

require 'DatabaseTestCases'
require 'ConnectionTestCases'
require 'CursorTestCases'
require 'DataTypesTestCases'
require 'NumericDataTypesTestCases'
require 'TransactionTestCases'
if RUBY_VERSION =~ /^1.9/
  require 'EncodingTestCases'
end
