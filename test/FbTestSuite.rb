$: << File.dirname(__FILE__)
$: << File.join(File.dirname(__FILE__),'..')
require 'test/unit'
require 'DatabaseTestCases'
require 'ConnectionTestCases'
require 'CursorTestCases'
require 'DataTypesTestCases'
require 'TransactionTestCases'
