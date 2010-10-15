module Resque
  module Plugins
    module DynamicPriority
      def set_queue_priority(queue, score)
        queue_group = get_queue_group(queue)
        redis.zadd("queue_group:#{queue_group}", score, queue) if queue_group
      end

      # Adds queue to priority group. Call this once all items have been enqueued.
      # This will active the priority queue.
      def prioritize(queue_group, queue)
        # Note: The call order is important here. If set_queue_priority was called
        # first, it would be possible to empty out the queue via pop, without realizing
        # that it's a priority queue, and so the queue would never get deleted.
        # Since there is no access to the queue until set_queue_priority is set
        # adding the queue to the group hash first can have no ill effect.
        redis.hset('queue-group-lookup', "queue: #{queue}", queue_group)
        set_queue_priority(queue, 0)
      end

      def queues(queue_group)
        # No need to return all queues, since empty queues are actively removed
        redis.zrange("queue_group:#{queue_group}", 0, 5)        
      end

      def get_queue_group(queue)
        redis.hget('queue-group-lookup', "queue: #{queue}")
      end
      
      def redis
        Redis.redis
      end
      
      module Resque
        alias_method :dynamic_priority_original_pop, :pop
        def pop(queue)
          # Priority queues only become active after all items have been added.
          # Therefore there are no race conditions involved when checking for an empty
          # priority queue, since once it's empty it will remain empty forever.
          item = dynamic_priority_original_pop(queue)
          
          if item.nil?
            # If queue is empty and it's a priority queue, remove the queue.
            # This is necessary because queues are dynamically created and we
            # want all existing queues to be active. Otherwise we'd be constantly
            # processing empty queues, and the number of queues would grow forever.
            queue_group = Resque::Plugins::DynamicPriority.get_queue_group(queue)
            if queue_group
              # Queue is a priority queue, since it belongs to a group, so it's safe to remove
              redis.zrem("queue_group:#{queue_group}", queue)
              remove_queue(queue)
            end
          end
          
          item
        end
        
        alias_method :dynamic_priority_original_push, :push
        def push(queue, item)
          # If it's a priority queue, pushing is only allowed before it's activated
          raise "Attempted to push to the activated priority queue: #{queue}" if
            Resque::Plugins::DynamicPriority.get_queue_group(queue)
          dynamic_priority_original_push(queue, item)
        end
      end
      
      module Worker
        alias_method :dynamic_priority_original_queues, :queues
        def queues
          if @queues[0].start_with? '@'
            # It's a queue group
            Resque::Plugins::DynamicPriority.queues(@queues[0])
          else
            dynamic_priority_original_queues
          end
        end
      end
      
      module DynamicPriorityJob
        def after_enqueue
          Resque::Plugins::DynamicPriority.touch_queue_priority(queue_from_class)
        end

        def around_perform(*args)
          queue = queue_from_class
          Resque::Plugins::DynamicPriority.set_queue_priority(queue, optimistic_score)
          yield *args
          Resque::Plugins::DynamicPriority.set_queue_priority(queue, accurate_score)
        end
        
        def queue_from_class
          klass = self.class
          klass.instance_variable_get(:@queue) ||
            (klass.respond_to?(:queue) and klass.queue)
        end        
      end
    end
  end
end

Resque.extend Resque::Plugins::DynamicPriority::Resque
Resque::Worker.extend Resque::Plugins::DynamicPriority::Worker
