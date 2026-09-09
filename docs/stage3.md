3.1 Design the persistent log format
What exactly gets written to disk?
How does one message become one record?
How do we know where one record ends?
What is an offset?
3.2 Implement append-only log writing
Open/create topic log
Serialize a record
Append it
Keep the write path simple
3.3 Integrate PRODUCE with the disk log
PRODUCE should append to disk
In-memory state should no longer be the only copy
3.4 Read the log back
Start broker
Read existing topic logs
Reconstruct readable messages
3.5 Restart persistence
Produce
Kill broker
Restart
FETCH
Verify messages survived
3.6 Durability and fsync()
Understand write() vs kernel page cache vs durable storage
Decide what guarantee our broker actually provides
3.7 Corruption/incomplete-record handling
What happens if the process dies halfway through a record?
Establish the recovery behavior that Stage 9 can later build upon.