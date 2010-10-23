require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "resque-dynamic-queues"
    gem.summary = %Q{Adds dynamic priority queues to Resque.}
    gem.description = %Q{This plugin does two things. It implements the concept of dynamic queues and it selects the currently slowest queue. Dynamic queues are queues that are automatically removed when they're empty, allowing queues to mirror transient objects in the app.}
    gem.email = "joshmh@gmail.com"
    gem.homepage = "http://github.com/joshmh/resque-dynamic-queues"
    gem.authors = ["Josh Harvey"]
    gem.add_development_dependency "yard", ">= 0"

    # Note: Until Resque redis-namespace dependency is updated, we have to use a forked
    # version.
    gem.add_dependency('resque-joshmh', '>= 1.12.0')
    
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = false
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/*_test.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

begin
  require 'yard'
  YARD::Rake::YardocTask.new do |conf|
    conf.options = ['-mmarkdown', '--readme README.md']
    conf.files = ['lib/**/*.rb', '-', 'LICENSE', 'README.md' ]
  end  
rescue LoadError
  task :yardoc do
    abort "YARD is not available. In order to run yardoc, you must: sudo gem install yard"
  end
end
