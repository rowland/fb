$: << File.dirname(__FILE__)
require 'test/unit'
%w{ Database Connection Cursor DataTypes }.each do |f|
  require "#{f}TestCases"
end
