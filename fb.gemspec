require 'rubygems'

Gem::Specification.new do |s|
  s.name = "fb"
  s.version = "0.8.0"
  s.date = "2017-08-15"
  s.summary = "Firebird database driver"
  s.description = "Ruby Firebird Extension Library"
  s.licenses = ["MIT"]
  s.requirements = "Firebird client library fbclient.dll, libfbclient.so or Firebird.framework."
  s.require_path = '.'
  s.author = "Brent Rowland"
  s.email = "rowland@rowlandresearch.com"
  s.homepage = "http://github.com/rowland/fb"
  s.test_file = "test/FbTestSuite.rb"
  s.has_rdoc = true
  s.extra_rdoc_files = ['README.md']
  s.rdoc_options << '--title' << 'Fb -- Ruby Firebird Extension' << '--main' << 'README.md' << '-x' << 'test'
  s.files = ['extconf.rb', 'fb.c', 'README.md', 'fb_extensions.rb'] + Dir.glob("test/*.rb")
  s.platform = case RUBY_PLATFORM
    when /win32/ then Gem::Platform::WIN32
  else
    Gem::Platform::RUBY
  end
  s.extensions = ['extconf.rb'] if s.platform == Gem::Platform::RUBY
end
