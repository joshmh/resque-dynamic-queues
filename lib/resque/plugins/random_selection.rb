require 'resque'

module Resque
  module Plugins
    # This plugin does two things. It implements the concept of dynamic queues
    # and it implements random queue selection. Dynamic queues are queues that
    # are automatically removed when they're empty, allowing queues to mirror
    # transient objects in the app.
    # Random queue selection means the worker will pick a queue at random. Since
    # all the queues are active (they have jobs), the first chosen random queue
    # will be usable.    
    module RandomSelection
      class ClosedQueueError < RuntimeError; end
      class QueueGroupNamingError < RuntimeError; end

      module Base
        NUMBER_OF_QUEUES  = 1
        
        extend self
        
        # Number of queues to return from +queues+ method. Conceptually we only
        # need to return one, because we immediately remove all empty queues, so
        # we expect any queue to have jobs in it. However, since the operation isn't
        # atomic, it's possible to still get an empty queue. If there's a lot of action
        # going on, and this happens a lot, returning more queues would reduce the
        # possibility of workers waiting for a new queue. Setting to high values
        # has a big impact on performance.
        attr_writer :number_of_queues
                
        # Adds queue to queue group. Call this once all items have been enqueued.
        # This will activate the queue. This should be called by the application
        # for dynamic queues (queues that will be run by RandomWorkers).
        def activate(queue_group, queue, speed = 1)
          # Note: The call order is important here. If the queue was added to the
          # group set first, it would be possible to empty out the queue via pop, without 
          # realizing that it's a dynamic queue, and so the queue would never get deleted.
          # Since there is no access to the queue until queue is added to group set
          # adding the queue to the group hash first can have no ill effect.
          redis.hset('queue-start-time', queue, Time.now.to_f.to_s)
          redis.hset('queue-speed', queue, speed)
          redis.hset('queue-group-lookup', queue, queue_group)
          redis.sadd("queue_group:#{queue_group}", queue)
          redis.hset('queue-work', queue, 0)
        end

        def remove_priority_queue(queue)
          # These are extra steps necessary to remove a priority queue,
          # Redis.remove_queue should still be called, as well.
          queue_group = redis.hget('queue-group-lookup', queue)
          if queue_group
            redis.srem("queue_group:#{queue_group}", queue)
            redis.hdel('queue-start-time', queue)
            redis.hdel('queue-speed', queue)
            redis.hdel('queue-work', queue)
            redis.hdel('queue-group-lookup', queue)
          end
        end
                
        def number_of_queues
          @number_of_queues ||= NUMBER_OF_QUEUES
        end
        
        def queue(queue_group)
          t = Time.now.to_f
          start_times = redis.hgetall('queue-start-time')
          return nil if start_times.empty?
          work_table = redis.hgetall('queue-work')
          speed_table = redis.hgetall('queue-speed')
          
          # Compute scores
          scores = start_times.map do |k,v|
            work  = work_table[k].to_f
            speed = speed_table[k].to_i
            delta = (t - v.to_f) * speed
            score = work / delta  # delta == 0.0 is ok, score will be Infinity
            [ k, score ]
          end
                    
          # Choose best score
          scores.min {|a,b| a.last <=> b.last }.first
        end

        # Units of work +queue+ did. Useful for testing
        def units_worked(queue)
          redis.hget('queue-work', queue)
        end
        
        def increment_work(queue)
          redis.hincrby('queue-work', queue, 1) if get_queue_group(queue)
        end 
        
        def queues(queue_group)
          qs = []
          1.upto(number_of_queues) { qs << queue(queue_group) }
          qs.compact.uniq
        end
        
        def get_queue_group(queue)
          redis.hget('queue-group-lookup', queue)
        end
      
        def queue_exists?(queue)
          redis.sismember('queues', queue)
        end
        
        def redis
          ::Resque.redis
        end
      end
      
      module Resque
        def pop(queue)
          # Dynamic queues only become active after all items have been added.
          # Therefore there are no race conditions involved when checking for an empty
          # dynamic queue, since once it's empty it will remain empty forever.
          item = super

          if redis.llen("queue:#{queue}") == 0
            # If queue is empty and it's a dynamic queue, remove the queue.
            # This is necessary because queues are dynamically created and we
            # want all existing queues to be active. Otherwise we'd be constantly
            # processing empty queues, and the number of queues would grow forever.
            queue_group = Plugins::RandomSelection::Base.get_queue_group(queue)
            if queue_group
              # Queue is a dynamic queue, since it belongs to a group, so it's safe to remove
              remove_queue(queue)
            end
          end
          
          item
        end
        
        def push(queue, item)
          # If it's a dynamic queue, pushing is only allowed before it's activated
          raise Plugins::RandomSelection::ClosedQueueError,
            "Attempted to push to the activated priority queue: #{queue}" if
            Plugins::RandomSelection::Base.get_queue_group(queue)
          super
        end
        
        def remove_queue(queue)
          Plugins::RandomSelection::Base.remove_priority_queue(queue)
          super
        end
      end
            
      class RandomSelectionWorker < ::Resque::Worker

        def queues
          queue = @queues.first
          raise ::Resque::Plugins::RandomSelection::QueueGroupNamingError, 
            "When using a RandomSelectionWorker, you must supply a queue group, instead " +
            "of a queue, and the queue group must be prefixed with @, as in @mailings" if
            !queue.start_with?('@')
            
          ::Resque::Plugins::RandomSelection::Base.queues(queue[1..-1])
        end
      end      
    end
  end
end

Resque.extend Resque::Plugins::RandomSelection::Resque
Resque.before_fork do |job|
  ::Resque::Plugins::RandomSelection::Base.increment_work(job.queue)
end
