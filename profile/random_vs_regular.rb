require File.dirname(__FILE__) + '/../test/test_helper'
require 'benchmark'

NUM_QUEUES = 100

class TestWorker
  def self.perform
    # Don't need to actually do anything
  end    
end

module QueueGenerator
  extend self
  
  FACTOR = 10
  PROFILES = [
    { :speed => 1, :job_count => 1 * FACTOR, :queue_count => 100 },
    { :speed => 2, :job_count => 10 * FACTOR, :queue_count => 10 },
    { :speed => 4, :job_count => 50 * FACTOR, :queue_count => 5 },
    { :speed => 8, :job_count => 100 * FACTOR, :queue_count => 1 },
  ]
  def generate_random_queues
    PROFILES.each do |profile|
      speed = profile[:speed]
      queue = "RandomQueue#{speed}"
      1.upto(profile[:queue_count] * profile[:job_count]) do
        Resque::Job.create(queue, TestWorker)
      end
      Resque::Plugins::RandomSelection::Base.activate('group1', queue, 1.0 / speed)
    end
  end
  
  def generate_regular_queues
    PROFILES.each do |profile|
      1.upto(profile[:queue_count] * profile[:job_count]) do
        queue = "RegularQueue#{rand(NUM_QUEUES)}"
        Resque::Job.create(queue, TestWorker)
      end
    end
  end
  
end

# Case 1: Distribute all jobs randomly across NUM_QUEUES. Call regular worker with all
# queues (*).

# Case 2: A queue for each profile. Call priority worker with queue group.

Resque.redis.flushall

Benchmark.bmbm do |x|
=begin
  QueueGenerator.generate_regular_queues
  regular_worker = Resque::Worker.new('*')
  puts "Number of regular queues: #{regular_worker.queues.size}"
  x.report("regular run:") do
    regular_worker.work(0)
  end
  Resque.redis.flushall
=end  
  QueueGenerator.generate_random_queues
  random_worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
  x.report("random run:") do
    random_worker.work(0)
  end
end

Resque.redis.flushall

=begin

# Now profile regular to see why it's taking longer:

puts 'Profiling...'

# Profile the code
QueueGenerator.generate_regular_queues
regular_worker = Resque::Worker.new('*')
puts "Number of regular queues: #{regular_worker.queues.size}"
result = RubyProf.profile do
  regular_worker.work(0)
end

# Print a graph profile to text
file = File.new('profile.txt', 'w')
printer = RubyProf::GraphPrinter.new(result)
printer.print(file, 4)

Resque.redis.flushall
=end
