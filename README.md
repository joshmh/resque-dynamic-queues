Depends on Resque 1.10 (However, requires redis-namespace 1.10)

# Resque Dynamic Queues

This plugin does two things. It implements the concept of dynamic queues
and it selects the currently slowest queue. Dynamic queues are queues that
are automatically removed when they're empty, allowing queues to mirror
transient objects in the app.

The plugin records work performed and elapsed time for each queue, and always
selects the queue with the lowest current performance to process next. Since
all the queues are active (they have jobs), the first selected queue
will be usable.    

## Usage

After `require 'resque'`:

`require 'resque/plugins/dynamic_queues'`

Then, for dynamic queues, once all jobs have been queued:
<code>
    Resque::Plugins::DynamicQueues::Base.activate('group1', 'queue3')
</code>

This adds the queue to a queue group.

To randomly process jobs from all queues in the queue group:
<code>
    worker = Resque::Plugins::DynamicQueues::Worker.new('@group1')
    worker.work
</code>

Note the "@" prefix. This indicates that **group1** is a queue group and not a queue.

_Written by Joshua Harvey_
