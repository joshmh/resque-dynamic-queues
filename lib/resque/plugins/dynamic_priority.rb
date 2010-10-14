module Resque
  module Plugins
    module DynamicPriority
      def set_queue_priority(queue_group, queue, score)
        redis.zadd("queue_group:#{queue_group}", score, queue)
      end

      # Creates queue and gives it initial score of zero
      # If queue exists, does nothing
      def touch_queue_priority(queue_group, queue)
        redis.zincrby("queue_group:#{queue_group}", 0, queue)
      end

      def queues(queue_group)
        # No need to return all queues, since empty queues are actively removed
        top_queues = redis.zrange("queue_group:#{queue_group}", 0, 5)        
      end

      def redis
        Redis.redis
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
          queue_group = queue_group_from_class
          Resque::Plugins::DynamicPriority.touch_queue_priority(queue_group, queue_from_class) if
            queue_group
        end

        def queue_group_from_class
          klass = self.class
          klass.instance_variable_get(:@queue_group) ||
            (klass.respond_to?(:queue_group) and klass.queue_group)
        end        

        def queue_group_from_class
          klass = self.class
          klass.instance_variable_get(:@queue) ||
            (klass.respond_to?(:queue) and klass.queue)
        end        
      end
    end
  end
end

Resque::Worker.extend Resque::Plugins::DynamicPriority::Worker