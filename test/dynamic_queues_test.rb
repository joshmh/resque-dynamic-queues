require File.dirname(__FILE__) + '/test_helper'

class DynamicQueuesTest < Test::Unit::TestCase
  include Resque::Helpers
  
  class TestJob
    def self.perform
      # Don't need to actually do anything
    end    
  end
    
  def setup
    Resque.redis.flushall
    Resque::Plugins::DynamicQueues::Base.number_of_queues = nil
    Time.test_mode = true
    srand
  end
  
  def teardown
    Time.test_mode = false
  end
  
  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::DynamicQueues)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 10
  end

  # Need to run all Resque tests after plugin is patched in
  def test_push_to_new_active_queue
    Resque::Plugins::DynamicQueues::Base.activate('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::DynamicQueues::ClosedQueueError) do
      Resque.push('queue1', 'item1')
    end
  end

  def test_push_to_existing_active_queue
    assert_nothing_raised { Resque.push('queue1', 'item1') }
    Resque::Plugins::DynamicQueues::Base.activate('group1', 'queue1', 1)
    assert_raise(Resque::Plugins::DynamicQueues::ClosedQueueError) do
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
      worker = Resque::Plugins::DynamicQueues::Worker.new('@group1')
    end
  end
  
  def test_set_number_of_queues
    assert_equal 1, Resque::Plugins::DynamicQueues::Base.number_of_queues
    Resque::Plugins::DynamicQueues::Base.number_of_queues = 7
    assert_equal 7, Resque::Plugins::DynamicQueues::Base.number_of_queues
    Resque::Plugins::DynamicQueues::Base.number_of_queues = 11
    assert_equal 11, Resque::Plugins::DynamicQueues::Base.number_of_queues
  end
  
  # TODO: Low level testing of queues, activate, remove_queue
  
  def test_pop_last_item_removes_queue
    Resque.push('queue1', 'item')
    Resque.push('queue1', 'item')
    Resque::Plugins::DynamicQueues::Base.activate('group1', 'queue1')
    Resque.pop('queue1')
    assert_equal 1, Resque.queues.size
    assert_equal 1, Resque::Plugins::DynamicQueues::Base.queues('group1').size
    Resque.pop('queue1')
    assert_equal 0, Resque.queues.size
    assert_equal 0, Resque::Plugins::DynamicQueues::Base.queues('group1').size
  end
  
  def test_work
    1.upto(100)   { Resque::Job.create(:queue1, TestJob) }
    1.upto(75)    { Resque::Job.create(:queue2, TestJob) }
    1.upto(50)    { Resque::Job.create(:queue3, TestJob) }
    Resque::Plugins::DynamicQueues::Base.activate('group1', :queue1, 4)
    Resque::Plugins::DynamicQueues::Base.activate('group1', :queue2, 1)
    Resque::Plugins::DynamicQueues::Base.activate('group1', :queue3, 2)
    worker = Resque::Plugins::DynamicQueues::Worker.new('@group1')
    
    pqueues = []
    worker.work(0) do |job|
      pqueues << job.queue
    end
    
    k = 20
    n1 = compute_first_n pqueues, 'queue1', k
    n2 = compute_first_n pqueues, 'queue2', k
    n3 = compute_first_n pqueues, 'queue3', k
    pqsize = pqueues.size
    
    assert_equal 34, n1
    assert_equal 134, n2
    assert_equal 68, n3
    
    assert_equal 100, pqueues.count('queue1')
    assert_equal 75,  pqueues.count('queue2')
    assert_equal 50,  pqueues.count('queue3')
  end

  def test_increment_work
    Resque::Plugins::DynamicQueues::Base.increment_work('queue1')

    # Shouldn't increment work because queue isn't dynamic
    assert_equal 0, Resque::Plugins::DynamicQueues::Base.units_worked('queue1').to_i
    
    # Make it dynamic, then test again
    Resque::Plugins::DynamicQueues::Base.activate('group1', "queue1")
    Resque::Plugins::DynamicQueues::Base.increment_work('queue1')
    assert_equal 1, Resque::Plugins::DynamicQueues::Base.units_worked('queue1').to_i
  end
  
  def test_work_increments
    Resque::Plugins::DynamicQueues::Base.increment_work('queue_test')
    
    Resque::Job.create(:queue1, TestJob)
    Resque::Job.create(:queue1, TestJob)
    Resque::Plugins::DynamicQueues::Base.activate('group1', "queue1")
    worker = Resque::Plugins::DynamicQueues::Worker.new('@group1')
    worker.work(0) do
      worker.pause_processing
    end
    assert_equal 1, Resque::Plugins::DynamicQueues::Base.units_worked('queue1').to_i
  end
  
  def test_no_starvation_dynamic_no_growth
    nq = 20       # Number of initial queues
    tq = 20
    nj = 10       # Jobs per queue
    niq = 0       # Number of new queues per iteration

    assert_equal 20, no_starvation_dynamic(nq, tq, nj, niq)
  end
  
  def test_no_starvation_dynamic_moderate_growth
    nq = 20       # Number of initial queues
    nj = 10       # Jobs per queue
    niq = 1
    tq = 100
    
    # random : scored : scored with fast_start and speeds    
    assert_equal 41, no_starvation_dynamic(nq, tq, nj, niq) # 63 / 3s : 38 / 3.7s : 41 / 3.6s
  end

  def test_no_starvation_dynamic_moderate2_growth
    nq = 100       # Number of initial queues
    tq = 110
    nj = 10       # Jobs per queue
    niq = 2
    
    assert_equal 210, no_starvation_dynamic(nq, tq, nj, niq) # 345 / 28s : 190 / 36s : 210 / 38s
  end

  def test_no_starvation_dynamic_hi_growth
    nq =  50       # Number of initial queues
    tq = 500
    nj = 10       # Jobs per queue
    niq = 2       # Number of new queues per iteration

    assert_equal 414, no_starvation_dynamic(nq, tq, nj, niq) # 841 / 36s : 342 / 94s : 414 / 152s (107s w/json)
  end

  def test_no_starvation_dynamic_super_hi_growth
    nq = 20       # Number of initial queues
    tq = 100      # Number of total queues
    nj = 10       # Jobs per queue
    niq = 4       # Number of new queues per iteration

    assert_equal 145, no_starvation_dynamic(nq, tq, nj, niq) # 237 / 10s : 130 / 21s : 145 / 17s
  end
  
  def no_starvation_dynamic(nq, tq, nj, niq)
    # Attempts to model a more realistic scenario where jobs are being added and
    # completed all the time.
    
    @jobs_to_work = 10
    @jobs_per_queue = nj
        
    # Generate initial queues
    1.upto(nq) do |i|
      queue = "initial_queue#{i}"
      1.upto(nj) { Resque::Job.create(queue, TestJob) }
      Resque::Plugins::DynamicQueues::Base.activate('group1', queue)
    end

    @worker = Resque::Plugins::DynamicQueues::Worker.new('@group1')
    @jobs_completed = {}
    @queues_completed = 0
    new_queue_count = 0
    
    iterations = niq == 0 ? 0 : (tq - nq) / niq
    work = 0
    1.upto(iterations) do |i|
      1.upto(niq) do |j|
        new_queue_count += 1
        queue = "new_queue#{new_queue_count}"
        1.upto(nj) { Resque::Job.create(queue, TestJob) }
        Resque::Plugins::DynamicQueues::Base.activate('group1', queue)
      end
      work += 1
      return work if work_until_done(nq)
    end
    
    loop do
      work += 1
      new_queue_count += 1
      if niq > 0
        queue = "new_queue#{new_queue_count}"
        1.upto(nj) { Resque::Job.create(queue, TestJob) }
        Resque::Plugins::DynamicQueues::Base.activate('group1', queue)
      end
      break if work_until_done(nq)
    end
    work
  end
  
  def work_until_done(nq)
    jobs_count = 0
    @worker.work(0) do |job|
      queue = job.queue
      if queue.start_with? 'initial'
        @jobs_completed[queue] ||= 0
        @jobs_completed[queue]  += 1
        if @jobs_completed[queue] == @jobs_per_queue
          @queues_completed += 1
          if @queues_completed == nq
            return true
          end
        end
      end
      jobs_count += 1
      @worker.pause_processing if jobs_count >= @jobs_to_work
    end
    @worker.unpause_processing
    false
  end
  
  def test_first_n
    assert_equal 6, compute_first_n(%w[ a b c a a j a w s a j s a ], 'a', 4)
  end
  
  def test_first_n_early_finish
    assert_nothing_raised { compute_first_n(%w[ a b c a a j a w s a j s a ], 'a', 6) }
    assert_raises(RuntimeError) { compute_first_n(%w[ a b c a a j a w s a j s a ], 'a', 7) }
  end
  
  def test_time
    Time.test_mode = true
    t0 = Time.now
    assert_equal 1, Time.now - t0
    assert_equal 2, Time.now - t0
    
    Time.test_mode = false
    t0 = Time.now
    sleep 0.1
    assert_in_delta 0.1, Time.now - t0, 0.05
  end
  
  # Compute the index in the array where n of the items of type +item+ were encountered
  def compute_first_n(array, item, n)
    index = 0; c = 0
    array.each {|i| c += 1 if i == item; break unless c < n; index += 1; }
    raise "Never got to #{n} appearances, ran through whole array" if
      c < n
    index
  end
end

