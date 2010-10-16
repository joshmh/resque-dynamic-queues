require File.dirname(__FILE__) + '/test_helper'

class DynamicPriorityTest < Test::Unit::TestCase
  include Resque::Helpers
#  include RubyProf::Test
  
  class TestWorker
    def self.perform
      # Don't need to actually do anything
    end    
  end
    
  def setup
    Resque.redis.namespace = "resque-dynamic-priority:test"
    Resque.redis.flushall
    srand
  end
  
  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::DynamicPriority)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 10
  end

  # Need to run all Resque tests after plugin is patched in
  def test_push_to_new_active_queue
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::DynamicPriority::ClosedQueueError) do
      Resque.push('queue1', 'item1')
    end
  end

  def test_push_to_existing_active_queue
    assert_nothing_raised { Resque.push('queue1', 'item1') }
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::DynamicPriority::ClosedQueueError) do
      Resque.push('queue1', 'item2')
    end
  end

  def test_push
    Resque.push('queue', 'item')
    assert_equal 'item', decode(Resque.redis.lpop("queue:queue"))
  end

  def test_pop
    Resque.push('queue', 'item')
    assert_equal 'item', Resque.pop('queue')
  end
    
  # Note: The algorithm used needs to be modified. All queues must be loaded
  # for each selection, along with start-time and work done. Scores must be
  # computed dynamically and the highest priority queue can then be selected.
  def test_prioritizing
    1.upto(100)  { Resque::Job.create(:queue1, TestWorker) }
    1.upto(100) { Resque::Job.create(:queue2, TestWorker) }
    1.upto(50) { Resque::Job.create(:queue3, TestWorker) }
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', :queue1, 1)
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', :queue2, 0.25)
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', :queue3, 0.5)
    worker = Resque::Plugins::DynamicPriority::PriorityWorker.new('@group1')
    
    pqueues = []
    worker.work(0) do |job|
      pqueues << job.queue
    end
    
    puts
    p pqueues
    k = 20
    puts
    puts "first #{k}, queue1: #{compute_halftime(pqueues, 'queue1', k)}"
    puts
    puts "first #{k}, queue2: #{compute_halftime(pqueues, 'queue2', k)}"
    puts
    puts "first #{k}, queue3: #{compute_halftime(pqueues, 'queue3', k)}"
  end
  
  # Compute the index in the array where half of the jobs of type +item+ were finished
  def compute_halftime(array, item, k)
    index = 0
    c = 0
    puts "number of jobs for #{item}: #{array.count(item)}"
    array.each {|i| c += 1 if i == item; break unless c < k; index += 1; }
    index
  end
end

