$: << File.dirname(__FILE__)
require 'test/unit'
%w{ Database Connection Cursor }.each do |f|
  require "#{f}TestCases"
end
