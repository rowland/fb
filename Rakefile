require "rake/testtask"
require "rake/extensiontask"

Rake::ExtensionTask.new "fb_ext" do |ext|
  ext.ext_dir = 'ext/fb'
  ext.lib_dir = 'lib/fb'
end

Rake::TestTask.new do |t|
  t.test_files = FileList['test/**/*.rb'] - FileList['test/test_helper.rb']
end

task :default => :test
