#!/usr/bin/env ruby
# ruby extconf.rb --with-opt-dir=e:/Firebird

require 'mkmf'

libs = %w/ gdslib gds /  # InterBase library

fbclientlib =  # Firebird library
  case RUBY_PLATFORM
  when /bccwin32/
    "fbclient_bor"
  when /mswin32/
    "fbclient_ms"
  else
    "fbclient"
  end
libs.push fbclientlib

test_func = "isc_attach_database"

# for ruby-1.8.1 mkmf
case RUBY_PLATFORM
when /win/
  libs.find {|lib| have_library(lib) } and
    have_func(test_func, ["ibase.h"])
else
  libs.find {|lib| have_library(lib, test_func) }
end and

create_makefile("fb")
