module Resque
  module Plugins
    module DynamicPriority      
      class ClosedQueueError < RuntimeError; end
      class QueueGroupNamingError < RuntimeError; end

      module Base
        # Number of times to attempt a random queue fetch before giving up
        # and returning what we have. Higher numbers will give us more accurate
        # modelling of the different queue probabilities, with the drawback
        # of an occasional batch of many Redis calls. This should be changed
        # as a function of the lowest probability queue in the system, but
        # is unlikely to have much effect on average performance.
        RANDOM_ATTEMPTS   = 20

        # Number of queues to return from +queues+ method. Conceptually we only
        # need to return one, because we immediately remove all empty queues, so
        # we expect any queue to have jobs in it. However, since the operation isn't
        # atomic, it's possible to still get an empty queue. If there's a lot of action
        # going on, and this happens a lot, returning more queues would reduce the
        # possibility of workers waiting for a new queue.
        NUMBER_OF_QUEUES  = 1
        
        extend self
                        
        # Adds queue to priority group. Call this once all items have been enqueued.
        # This will active the priority queue. This should be called by the application.
        def prioritize(queue_group, queue, probability)
          # Note: The call order is important here. If set_queue_priority was called
          # first, it would be possible to empty out the queue via pop, without realizing
          # that it's a priority queue, and so the queue would never get deleted.
          # Since there is no access to the queue until queue is added to group set
          # adding the queue to the group hash first can have no ill effect.
          redis.hset('queue-group-lookup', queue, queue_group)
          redis.hset('queue-probability', queue, probability.to_s)
          redis.rpush('queue-new', queue)
          redis.sadd("queue_group:#{queue_group}", queue)
        end

        def remove_priority_queue(queue)
          # These are extra steps necessary to remove a priority queue,
          # Redis.remove_queue should still be called, as well.
          queue_group = redis.hget('queue-group-lookup', queue)
          if queue_group
            redis.hdel('queue-group-lookup', queue)
            redis.hdel('queue-probability', queue)
            redis.srem("queue_group:#{queue_group}", queue)
          end
        end

        def random_attempts
          @random_attempts ||= RANDOM_ATTEMPTS
        end
        
        def random_attempts=(ra)
          @random_attempts = ra
        end
        
        def number_of_queues
          @number_of_queues ||= NUMBER_OF_QUEUES
        end

        def number_of_queues=(nq)
          @number_of_queues = nq
        end
        
        def queue(queue_group)
          # First check for brand new queues
          q = redis.lpop('queue-new')
          return q if q
          
          i = 0
          loop do
            queue = redis.srandmember("queue_group:#{queue_group}")
            break queue if i > random_attempts
            probability = redis.hget('queue-probability', queue).to_f
            break queue if rand < probability
            i += 1
          end
        end

        def queues(queue_group)
          qs = []
          1.upto(number_of_queues) { qs << queue(queue_group) }
          qs.uniq
        end
        
        def get_queue_group(queue)
          redis.hget('queue-group-lookup', queue)
        end
      
        def redis
          ::Resque.redis
        end
      end
      
      module Resque
        def pop(queue)
          # Priority queues only become active after all items have been added.
          # Therefore there are no race conditions involved when checking for an empty
          # priority queue, since once it's empty it will remain empty forever.
          item = super

          if redis.llen("queue:#{queue}") == 0
            # If queue is empty and it's a priority queue, remove the queue.
            # This is necessary because queues are dynamically created and we
            # want all existing queues to be active. Otherwise we'd be constantly
            # processing empty queues, and the number of queues would grow forever.
            queue_group = Plugins::DynamicPriority::Base.get_queue_group(queue)
            if queue_group
              # Queue is a priority queue, since it belongs to a group, so it's safe to remove
              remove_queue(queue)
            end
          end
          
          item
        end
        
        def push(queue, item)
          # If it's a priority queue, pushing is only allowed before it's activated
          raise Plugins::DynamicPriority::ClosedQueueError,
            "Attempted to push to the activated priority queue: #{queue}" if
            Plugins::DynamicPriority::Base.get_queue_group(queue)
          super
        end
        
        def remove_queue(queue)
          Plugins::DynamicPriority::Base.remove_priority_queue(queue)
          super
        end
      end
            
      class PriorityWorker < ::Resque::Worker
        def queues
          queue = @queues.first
          raise ::Resque::Plugins::DynamicPriority::QueueGroupNamingError, 
            "When using a PriorityWorker, you must supply a queue group, instead " +
            "of a queue, and the queue group must be prefixed with @, as in @mailings" if
            !queue.start_with?('@')
            
          ::Resque::Plugins::DynamicPriority::Base.queues(queue[1..-1])
        end
      end      
    end
  end
end

Resque.extend Resque::Plugins::DynamicPriority::Resque
