# require 'resque/plugins/dynamic_queues/tasks'
# will give you the dynamic-queues resque tasks

namespace :resque do
  namespace :dynamic_queues do
    task :setup

    desc "Start a Dynamic Queues Resque worker"
    task :work => :setup do
      require 'resque/plugins/dynamic_queues'

      worker = nil
      queue_group = (ENV['QUEUE_GROUP']).to_s

      begin
        worker = Resque::Plugins::DynamicQueues::Worker.new(queue_group)
        worker.verbose = ENV['LOGGING'] || ENV['VERBOSE']
        worker.very_verbose = ENV['VVERBOSE']
      rescue Resque::NoQueueError, Resque::Plugins::DynamicQueues::QueueGroupNamingError
        abort "set QUEUE_GROUP env var, e.g. $ QUEUE_GROUP=@mailing rake resque:dynamic_queues:work"
      end

      worker.log "Starting worker #{worker}"

      worker.work(ENV['INTERVAL'] || 5) # interval, will block
    end

    desc "Start multiple Dynamic Queues Resque workers. Should only be used in dev mode."
    task :workers do
      threads = []

      ENV['COUNT'].to_i.times do
        threads << Thread.new do
          system "rake resque:dynamic_queues:work"
        end
      end

      threads.each { |thread| thread.join }
    end
  end
end
