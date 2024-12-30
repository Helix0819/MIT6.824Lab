package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	RequestId   string // Sequence number
	MessageType string
}

type PutAppendReply struct {
	Value string
	Err   string // Error message if any
}

type GetArgs struct {
	Key         string
	RequestId   string // Sequence number
	MessageType string
}

type GetReply struct {
	Value string
	Err   string // Error message if any
}

type KeyValue struct {
	Key   string
	Value string
}

type Message struct {
	RequestType string
	ReportType  string
}
