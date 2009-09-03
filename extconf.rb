#!/usr/bin/env ruby
# = Windows
# === Sample of Makefile creation:
# <tt>ruby extconf.rb --with-opt-dir=c:/Firebird --with-dotnet-dir=C:\PROGRA~1\MICROS~2.NET\Vc7 --with-win32-dir=C:\PROGRA~1\MI9547~1</tt>
# === Notes
# * Windows build currently only compiles using VC7 (Visual Studio .NET 2003).
# * mkmf doesn't like directories with spaces, hence the 8.3 notation in the example above.
# = Linux (Intel)
# === Notes
# * Build seems to "just work."
# * Unit tests take about 10 times as long to complete using Firebird Classic.  Default xinetd.conf settings may not allow the tests to complete due to the frequency with which new attachments are made.
# = Linux (Other)
# * Volunteers?
# = Mac OS X (PowerPC)
# * Not currently tested.
# = Mac OS X (Intel)
# * Works
require 'mkmf'

libs = %w/ fbclient gds /

case RUBY_PLATFORM
  when /bccwin32/
    libs.push "fbclient_bor"
  when /mswin32/
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
when /mswin32/
  libs.find {|lib| have_library(lib) } and
    have_func(test_func, ["ibase.h"])
else
  libs.find {|lib| have_library(lib, test_func) }
end

create_makefile("fb")
