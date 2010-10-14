module Resque
  module Plugins
    module DynamicPriority
      
      def push(queue, item, gang = nil)
        watch_queue(queue)
        if gang
          redis.rpush("pqueue:#{queue},gang:#{gang}", encode(item))
          set_gang(queue, gang)
        else
          redis.rpush("queue:#{queue}", encode(item))
        end
      end

      def set_gang(queue, gang, score = 0)
        redis.zadd("pqueue:#{queue}", score, gang)
      end
      
      def pop(queue)
        item = redis.lpop("queue:#{queue}")

        # If no regular queue item is found, it might be a priority queue
        item ||= do
          pqueue_key = "pqueue:#{queue}"
          
          # Then pop an item for that gang
          loop do
            # Get highest priority gang
            gang_list = redis.zrange(pqueue_key, 0, 0)
            gang = gang_list && gang_list.first
            break nil unless gang  # Queue is empty, no more gangs

            item = redis.lpop("pqueue:#{queue},gang:#{gang}")

            # Delete gang if it's empty
            redis.zrem(pqueue_key, gang) unless item

            break item if item
          end
        end
        decode item
      end
    end
  end
end

Resque.extend Resque::Plugins::DynamicPriority