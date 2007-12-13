#!/bin/env ruby
require 'rubygems'

spec = Gem::Specification.new do |s|
  s.name = "fb"
  s.version = "0.5.3"
  s.date = "2007-12-11"
  s.summary = "Firebird and Interbase driver"
  s.requirements = "Firebird client library fbclient.dll"
  s.require_path = '.'
  s.email = "rowland@rowlandresearch.com"
  s.homepage = "http://www.rowlandresearch.com/ruby/"
  s.rubyforge_project = "fb"
  s.test_file = "test/FbTestSuite.rb"
  s.has_rdoc = true
  s.extra_rdoc_files = ['README']
  s.rdoc_options << '--title' << 'Fb -- Ruby Firebird Extension' << '--main' << 'README' << '-x' << 'test'
  s.files = ['extconf.rb', 'fb.c', 'README'] + Dir.glob("test/*")
  s.files << (PLATFORM =~ /darwin/) ? 'fb.bundle' : 'fb.so'
  s.platform = case PLATFORM
    when /win32/ then Gem::Platform::WIN32
    when /linux/ then Gem::Platform::LINUX_586
    when /i686-darwin/ then "i686-darwin"
    when /powerpc-darwin/ then Gem::Platform::DARWIN
  end
end

if __FILE__ == $0
  Gem.manage_gems
  Gem::Builder.new(spec).build
end
