Depends on Resque 1.10

# Resque Random Selection

This plugin does two things. It implements the concept of dynamic queues
and it implements random queue selection. Dynamic queues are queues that
are automatically removed when they're empty, allowing queues to mirror
transient objects in the app.

Random queue selection means the worker will pick a queue at random. Since
all the queues are active (they have jobs), the first chosen random queue
will be usable.

Written by Joshua Harvey
