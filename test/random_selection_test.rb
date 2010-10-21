require File.dirname(__FILE__) + '/test_helper'

class RandomSelectionTest < Test::Unit::TestCase
  include Resque::Helpers
  
  class TestJob
    def self.perform
      # Don't need to actually do anything
    end    
  end
    
  def setup
    Resque.redis.flushall
    Resque::Plugins::RandomSelection::Base.number_of_queues = nil
    Resque::Plugins::RandomSelection::Base.quick_start_factor = nil

    srand
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

  def test_queue_exists
    Resque.push('queue1', 'item')
    assert Resque::Plugins::RandomSelection::Base.queue_exists?('queue1')
    assert !Resque::Plugins::RandomSelection::Base.queue_exists?('queue2')
    assert Resque::Plugins::RandomSelection::Base.queue_exists?('queue1')
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue1')
    Resque.pop('queue1')
    assert !Resque::Plugins::RandomSelection::Base.queue_exists?('queue1')    
  end
  
  # TODO: Low level testing of queues, activate, remove_queue

  # Checks to see if the quick start queue is getting cleaned up as
  # we pop off queues.
  def test_queue_new_handles_emptied_queues
    Resque::Plugins::RandomSelection::Base.quick_start_factor = 1.0
    assert_equal 0, Resque.redis.llen('queue-new')

    # Populate queues
    Resque.push('queue1', 'item')
    Resque.push('queue2', 'item')
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue1')
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue2')

    # Pop from the first queue, extinguishing the queue
    Resque.pop('queue1')
    assert !Resque::Plugins::RandomSelection::Base.queue_exists?('queue1')    

    # Since we never asked for a queue yet, neither queue was popped from queue-new
    assert_equal 2, Resque.redis.llen('queue-new')    

    # Now we request a queue, this should pull off the first queue, discard it, and
    # give us the second queue
    assert_equal 'queue2', Resque::Plugins::RandomSelection::Base.queue('group1')
    assert_equal 0, Resque.redis.llen('queue-new')    
  end
  
  def test_pop_last_item_removes_queue
    Resque.push('queue1', 'item')
    Resque.push('queue1', 'item')
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue1')
    Resque.pop('queue1')
    assert_equal 1, Resque.queues.size
    assert_equal 1, Resque::Plugins::RandomSelection::Base.queues('group1').size
    Resque.pop('queue1')
    assert_equal 0, Resque.queues.size
    assert_equal 0, Resque::Plugins::RandomSelection::Base.queues('group1').size
  end
  
  def test_work
    1.upto(100)   { Resque::Job.create(:queue1, TestJob) }
    1.upto(75)    { Resque::Job.create(:queue2, TestJob) }
    1.upto(50)    { Resque::Job.create(:queue3, TestJob) }
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue1, 1)
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue2, 0.25)
    Resque::Plugins::RandomSelection::Base.activate('group1', :queue3, 0.5)
    worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    
    pqueues = []
    worker.work(0) do |job|
      pqueues << job.queue
    end
    
    k = 20
    n1 = compute_first_n pqueues, 'queue1', k
    n2 = compute_first_n pqueues, 'queue2', k
    n3 = compute_first_n pqueues, 'queue3', k
    pqsize = pqueues.size
    n_total = (pqsize - n1) + (pqsize - n2) + (pqsize - n3)    # Sum of queues distributions
    p_total = 1 + 0.25 + 0.5  # Sum of queue probabilities
    
    assert_in_delta 1.0  / p_total, (pqsize - n1).to_f / n_total, 0.2
    assert_in_delta 0.25 / p_total, (pqsize - n2).to_f / n_total, 0.2
    assert_in_delta 0.5  / p_total, (pqsize - n3).to_f / n_total, 0.2
    
    assert_equal 100, pqueues.count('queue1')
    assert_equal 75,  pqueues.count('queue2')
    assert_equal 50,  pqueues.count('queue3')
  end
  
  def test_no_starvation
    # Note: This isn't a rigorous statistical test. If it happens to fail but is reasonably
    # close to the expected values, it still works.
    
    # Make the test harder, don't let queues quick start
    Resque::Plugins::RandomSelection::Base.quick_start_factor = 0.0

    n   = 20  # Number of queues
    k   = 3   # Number of queue appearances to sample for
    nj  = 30  # Number of jobs per queue

    1.upto(n) do |i|
      1.upto(30) { Resque::Job.create("queue#{i}", TestJob) }
      Resque::Plugins::RandomSelection::Base.activate('group1', "queue#{i}")
    end

    worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    
    pqueues = []
    worker.work(0) do |job|
      pqueues << job.queue
    end
    pqueue_size = pqueues.size
    
    # All queues should get a chance after a number of iterations that's
    # reasonably close to the number of total queues.
    max_expected = n * k * 2
    1.upto(n) do |i|
      first_n = compute_first_n(pqueues, "queue#{i}", k)
      assert first_n < max_expected, 
        "Queue#{i} only showed up #{k} times #{(first_n * 100) / pqueue_size}% of the " +
        "way through. #{first_n} was expected to be less than #{max_expected}."
    end
  end

  def test_no_starvation_dynamic_no_growth
    nq = 20       # Number of initial queues
    nj = 10       # Jobs per queue
    niq = 0       # Number of new queues per iteration

    p no_starvation_dynamic(nq, nj, niq) # 20
  end
  
  def test_no_starvation_dynamic_moderate_growth
    nq = 20       # Number of initial queues
    nj = 10       # Jobs per queue
    niq = 1
    tq = 100
    
    p no_starvation_dynamic(nq, tq, nj, niq) # 63
  end

  def test_no_starvation_dynamic_hi_growth
    nq =  50       # Number of initial queues
#    nq = 20       # Number of initial queues
    tq = 500
    nj = 10       # Jobs per queue
    niq = 2       # Number of new queues per iteration

#    p no_starvation_dynamic(nq, nj, niq) # 387
    p no_starvation_dynamic(nq, tq, nj, niq) # 49
  end

  def test_no_starvation_dynamic_super_hi_growth
    nq = 20       # Number of initial queues
    tq = 100      # Number of total queues
    nj = 10       # Jobs per queue
    niq = 4       # Number of new queues per iteration

    p no_starvation_dynamic(nq, tq, nj, niq) # 741
  end
  
  def no_starvation_dynamic(nq, tq, nj, niq)
    # Attempts to model a more realistic scenario where jobs are being added and
    # completed all the time.

    # Make the test harder, don't let queues quick start
    Resque::Plugins::RandomSelection::Base.quick_start_factor = 0.0
    
    @jobs_per_queue = {}
    @jobs_to_work = 10
    
    # Generate initial queues
    1.upto(nq) do |i|
      # Decent exponential distribution of jobs per queue
      nj = (( 2 ** ( 10 * rand )  / 10 ) + 1 ).to_i

      queue = "initial_queue#{i}"
      @jobs_per_queue[queue] = nj
      1.upto(nj) { Resque::Job.create(queue, TestJob) }
      Resque::Plugins::RandomSelection::Base.activate('group1', queue)
    end

    @worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    @jobs_completed = {}
    @queues_completed = 0
    new_queue_count = 0
    
    iterations = (tq - nq) / niq
    work = 0
    1.upto(iterations) do |i|
      1.upto(niq) do |j|
        new_queue_count += 1
        queue = "new_queue#{new_queue_count}"

        # Decent exponential distribution of jobs per queue
        nj = (( 2 ** ( 10 * rand )  / 10 ) + 1 ).to_i

        1.upto(nj) { Resque::Job.create(queue, TestJob) }
        Resque::Plugins::RandomSelection::Base.activate('group1', queue)
      end
      work += 1
      return work if work_until_done(nq)
    end
    
    loop do
      work += 1
      new_queue_count += 1
      queue = "new_queue#{new_queue_count}"
      1.upto(nj) { Resque::Job.create(queue, TestJob) }
      Resque::Plugins::RandomSelection::Base.activate('group1', queue)
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
        if @jobs_completed[queue] == @jobs_per_queue[queue]
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
  
  # Compute the index in the array where n of the items of type +item+ were encountered
  def compute_first_n(array, item, n)
    index = 0; c = 0
    array.each {|i| c += 1 if i == item; break unless c < n; index += 1; }
    raise "Never got to #{n} appearances, ran through whole array" if
      c < n
    index
  end
end

