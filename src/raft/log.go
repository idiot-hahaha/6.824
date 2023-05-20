package raft

type Log struct {
	Entries    []Entry
	StartIndex int
}

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}

func (log *Log) getLastLogIndex() int {
	return log.StartIndex + len(log.Entries) - 1
}

func (log *Log) getLastLogTerm() int {
	return log.Entries[len(log.Entries)-1].Term
}

func (log *Log) getLogTermByIndex(index int) int {
	return log.Entries[index-log.StartIndex].Term
}

func (log *Log) getCommandByIndex(index int) interface{} {
	return log.Entries[index-log.StartIndex].Command
}

func (log *Log) sliceLog(start, end int) Log {
	return Log{
		Entries:    log.Entries[start-log.StartIndex-1 : end-log.StartIndex],
		StartIndex: start - 1,
	}
}

func (log *Log) sliceLogHead(end int) Log {
	return Log{
		Entries:    log.Entries[:end-log.StartIndex],
		StartIndex: log.StartIndex,
	}
}

func (log *Log) sliceLogTail(start int) Log {
	return Log{
		Entries:    log.Entries[start-log.StartIndex-1:],
		StartIndex: start - 1,
	}
}

func (log *Log) sliceEntries(start, end int) []Entry {
	return log.Entries[start-log.StartIndex : end-log.StartIndex]
}

func (log *Log) sliceEntriesTail(start int) []Entry {
	return log.Entries[start-log.StartIndex:]
}

func (log *Log) sliceEntriesHead(end int) []Entry {
	return log.Entries[1 : end-log.StartIndex]
}

func (log *Log) appendLog(entries []Entry) Log {
	return Log{
		Entries:    append(log.Entries, entries...),
		StartIndex: log.StartIndex,
	}
}

func makeEmptyLog() Log {
	return Log{
		Entries: []Entry{
			{
				nil,
				0,
				0,
			},
		},
		StartIndex: 0,
	}
}

func (log *Log) appendLogByArgs(args AppendEntriesArgs) {

	if args.PrevLogIndex+len(args.Entries) > log.getLastLogIndex() {
		log.Entries = append(log.Entries[:args.PrevLogIndex-log.StartIndex])
	}
}
