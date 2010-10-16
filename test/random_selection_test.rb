require File.dirname(__FILE__) + '/test_helper'

class RandomSelectionTest < Test::Unit::TestCase
  include Resque::Helpers
  
  class TestWorker
    def self.perform
      # Don't need to actually do anything
    end    
  end
    
  def setup
    Resque.redis.flushall
    Resque::Plugins::RandomSelection::Base.number_of_queues = nil
  end
  
  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::RandomSelection)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 10
  end

  # Need to run all Resque tests after plugin is patched in
  def test_push_to_new_active_queue
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::RandomSelection::ClosedQueueError) do
      Resque.push('queue1', 'item1')
    end
  end

  def test_push_to_existing_active_queue
    assert_nothing_raised { Resque.push('queue1', 'item1') }
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::RandomSelection::ClosedQueueError) do
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
  
  def test_run_without_queue_setup
    assert_nothing_raised do
      worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    end
  end
  
  def test_set_number_of_queues
    assert_equal 1, Resque::Plugins::RandomSelection::Base.number_of_queues
    Resque::Plugins::RandomSelection::Base.number_of_queues = 7
    assert_equal 7, Resque::Plugins::RandomSelection::Base.number_of_queues
    Resque::Plugins::RandomSelection::Base.number_of_queues = 11
    assert_equal 11, Resque::Plugins::RandomSelection::Base.number_of_queues
  end
      
  # Note: The algorithm used needs to be modified. All queues must be loaded
  # for each selection, along with start-time and work done. Scores must be
  # computed dynamically and the highest priority queue can then be selected.
  def test_work
    1.upto(100)   { Resque::Job.create(:queue1, TestWorker) }
    1.upto(100)   { Resque::Job.create(:queue2, TestWorker) }
    1.upto(50)    { Resque::Job.create(:queue3, TestWorker) }
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue1, 1)
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue2, 0.25)
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue3, 0.5)
    worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    
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

