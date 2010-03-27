#!/usr/bin/env ruby
# = Windows
# === Sample of Makefile creation:
# <tt>ruby extconf.rb --with-opt-dir=C:/Progra~1/Firebird/Firebird_2_1</tt>
# === Notes
# * Windows is known to build with Ruby 1.8.7 from rubyinstaller.org.
# * New in this release is automatically finding your Firebird install under Program Files.
# * If your install is some place non-standard (or on a non-English version of Windows), you'll need to run extconf.rb manually as above.
# * mkmf doesn't like directories with spaces, hence the 8.3 notation in the example above.
# = Linux
# === Notes
# * Build seems to "just work."
# * Unit tests take about 10 times as long to complete using Firebird Classic.  Default xinetd.conf settings may not allow the tests to complete due to the frequency with which new attachments are made.
# = Mac OS X (Intel)
# * Works

if RUBY_PLATFORM =~ /(mingw32|mswin32)/ and ARGV.grep(/^--with-opt-dir=/).empty?
  program_files = ENV['ProgramFiles'].gsub('\\', '/').gsub(/(\w+\s+[\w\s]+)/) { |s| s.size > 8 ? s[0,6] + '~1' : s }
  if opt = Dir["#{program_files}/Firebird/Firebird_*"].sort.last
    ARGV << "--with-opt-dir=#{opt}"
  end
end

require 'mkmf'

libs = %w/ fbclient gds /

case RUBY_PLATFORM
  when /bccwin32/
    libs.push "fbclient_bor"
  when /mswin32/, /mingw32/
    $CFLAGS  = $CFLAGS + " -DOS_WIN32"
    libs.push "fbclient_ms"
  when /darwin/
    hosttype = `uname -m`.chomp
    $CFLAGS += " -DOS_UNIX"
#    $CFLAGS.gsub!(/-arch (\w+)/) { |m| $1 == hosttype ? m : '' }
#    $LDFLAGS.gsub!(/-arch (\w+)/) { |m| $1 == hosttype ? m : '' }
#    CONFIG['LDSHARED'].gsub!(/-arch (\w+)/) { |m| $1 == hosttype ? m : '' }
    $CPPFLAGS += " -I/Library/Frameworks/Firebird.framework/Headers"
    $LDFLAGS += " -framework Firebird"
  when /linux/
    $CFLAGS  = $CFLAGS + " -DOS_UNIX"
end

dir_config("firebird")

test_func = "isc_attach_database"

case RUBY_PLATFORM
when /mswin32/, /mingw32/
  libs.find {|lib| have_library(lib) } and
    have_func(test_func, ["ibase.h"])
else
  libs.find {|lib| have_library(lib, test_func) }
end

create_makefile("fb")
