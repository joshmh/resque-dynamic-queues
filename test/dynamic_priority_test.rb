require File.dirname(__FILE__) + '/test_helper'

class DynamicPriorityTest < Test::Unit::TestCase
  include Resque::Helpers
  
  class SampleWorker
    def self.perform
      # Don't need to actually do anything
    end    
  end
  
  def setup
    Resque.redis.namespace = "resque-dynamic-priority:test"
    Resque.redis.flushall
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
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', 'queue1')
    assert_raise(Resque::Plugins::DynamicPriority::ClosedQueueError) do
      Resque.push('queue1', 'item1')
    end
  end

  def test_push_to_existing_active_queue
    assert_nothing_raised { Resque.push('queue1', 'item1') }
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', 'queue1')
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
  
  def test_job_override
    job = Resque::Job.new('queue', 'payload')
    job.perform
  end
  
  def test_prioritizing
    1.upto(10) { Resque::Job.create(:queue1, SampleWorker) }
    1.upto(10) { Resque::Job.create(:queue2, SampleWorker) }
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', :queue1)
    Resque::Plugins::DynamicPriority::Base.prioritize('group1', :queue2)
    worker = Resque::Plugins::DynamicPriority::PriorityWorker.new('@group1')
    
    pqueues = []
    worker.work(0) do |job|
      pqueues << job.queue
    end
    
    p pqueues
  end
end

