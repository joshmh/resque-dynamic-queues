Depends on Resque 1.10

# Resque Random Selection

This plugin does two things. It implements the concept of dynamic queues
and it implements random queue selection. Dynamic queues are queues that
are automatically removed when they're empty, allowing queues to mirror
transient objects in the app.

Random queue selection means the worker will pick a queue at random. Since
all the queues are active (they have jobs), the first chosen random queue
will be usable.

## Usage

After `require 'resque'`:

`require 'resque/plugins/random_selection'`

Then, for dynamic queues, once all jobs have been queued:
<code>
    Resque::Plugins::RandomSelection::Base.activate('group1', 'queue3')
</code>

This adds the queue to a queue group.

To randomly process jobs from all queues in the queue group:
<code>
    worker = Resque::Plugins::RandomSelection::RandomSelectionWorker.new('@group1')
    worker.work
</code>

Note the "@" prefix. This indicates that **group1** is a queue group and not a queue.

_Written by Joshua Harvey_
