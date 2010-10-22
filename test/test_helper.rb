dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'
$TESTING = true
require 'test/unit'
require 'rubygems'
require 'resque'
require 'resque/plugins/dynamic_queues'

begin
  require 'leftright'
rescue LoadError
end

# Hack Time for testing. Increments every time Time.now is called, so we times
# are always at least a second apart.
class Time
  class << self
    attr_accessor :test_mode
    alias_method :now_orig, :now
    def now
      if test_mode
        increment
        @incremented_time
      else
        now_orig
      end
    end
    
    def increment(incr = 1)
      @incremented_time ||= now_orig
      @incremented_time += incr
    end  
  end
end

module TestTime
end

#
# make sure we can run redis
#

if !system("which redis-server")
  puts '', "** can't find `redis-server` in your path"
  puts "** try running `sudo rake install`"
  abort ''
end


#
# start our own redis when the tests start,
# kill it when they end
#

at_exit do
  next if $!

  if defined?(MiniTest)
    exit_code = MiniTest::Unit.new.run(ARGV)
  else
    exit_code = Test::Unit::AutoRunner.run
  end

  pid = `ps -A -o pid,command | grep [r]edis-test`.split(" ")[0]
  puts "Killing test redis server..."
  `rm -f #{dir}/dump.rdb`
  Process.kill("KILL", pid.to_i)
  exit exit_code
end

puts "Starting redis for testing at localhost:9736..."
`redis-server #{dir}/redis-test.conf`
Resque.redis = 'localhost:9736'
