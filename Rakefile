require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "resque-random-selection"
    gem.summary = %Q{Adds random selection from dynamic queues to Resque.}
    gem.description = %Q{This plugin does two things. It implements the concept of dynamic queues and it implements random queue selection. Dynamic queues are queues that are automatically removed when they're empty, allowing queues to mirror transient objects in the app.}
    gem.email = "joshmh@gmail.com"
    gem.homepage = "http://github.com/joshmh/resque-random-selection"
    gem.authors = ["Josh Harvey"]
    gem.add_dependency('resque', '>= 1.10.0')
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

task :default => :test

def command?(command)
  system("type #{command} > /dev/null")
end

require 'rake/testtask'
if command? :turn
  desc "Run tests"
  task :test do
    suffix = "-n #{ENV['TEST']}" if ENV['TEST']
    sh "turn test/*.rb #{suffix}"
  end
else
  Rake::TestTask.new do |t|
    t.libs << 'lib'
    t.pattern = 'test/**/*_test.rb'
    t.verbose = true
  end
  task :test => :check_dependencies
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "resque-random-selection #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
