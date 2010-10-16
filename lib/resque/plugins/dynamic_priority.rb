module Resque
  module Plugins
    module DynamicPriority      
      class ClosedQueueError < RuntimeError; end

      module Base
        extend self
        
        def update_priority(queue)
          priority = redis.hincrby('queue-work-done', queue, 1)
          start_time = redis.hget('queue-start-time', queue).to_i
          delta = Time.now.to_i - start_time
          delta = 1 if delta == 0
          puts "Updating priority to: #{priority.to_f / delta}" # DEBUG
          set_queue_priority(queue, priority.to_f / delta)
        end
        
        def set_queue_priority(queue, score)
          queue_group = get_queue_group(queue)
          redis.zadd("queue_group:#{queue_group}", score, queue) if queue_group
        end

        def remove_priority_queue(queue)
          # These are extra steps necessary to remove a priority queue,
          # Redis.remove_queue should still be called, as well.
          queue_group = redis.hget('queue-group-lookup', queue)
          if queue_group
            redis.hdel('queue-group-lookup', queue)
            redis.zrem("queue_group:#{queue_group}", queue)
            redis.hdel('queue-work-done', queue)
            redis.hdel('queue-start-time', queue)
          end
        end
        
        # Adds queue to priority group. Call this once all items have been enqueued.
        # This will active the priority queue.
        def prioritize(queue_group, queue)
          # Note: The call order is important here. If set_queue_priority was called
          # first, it would be possible to empty out the queue via pop, without realizing
          # that it's a priority queue, and so the queue would never get deleted.
          # Since there is no access to the queue until set_queue_priority is set
          # adding the queue to the group hash first can have no ill effect.
          redis.hset('queue-group-lookup', queue, queue_group)
          redis.hset('queue-start-time', queue, Time.now.to_i)
          set_queue_priority(queue, 0)
        end

        def queue(queue_group)
          queue_group = queue_group[1..-1]  # lop off @
          p redis.zrange("queue_group:#{queue_group}", 0, 5)
          redis.zrange("queue_group:#{queue_group}", 0, 0).first
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
          item = super(queue)

          if item.nil?
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
        def reserve
          queue = ::Resque::Plugins::DynamicPriority::Base.queue(@queues.first)
          puts "reserving #{queue}"
          return nil unless queue
          job = ::Resque::Job.reserve(queue)
          ::Resque::Plugins::DynamicPriority::Base.update_priority(queue) if job
          job
        end
      end      
    end
  end
end

Resque.extend Resque::Plugins::DynamicPriority::Resque
