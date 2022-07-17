package replication

type MessageType string

const (
	ChunkCreated      MessageType = "chunk_created"
	ChunkAcknowledged MessageType = "chunk_ack"
)

type Peer struct {
	InstanceName string
	ListenAddr   string
}

type Chunk struct {
	Owner    string
	Category string
	FileName string
}

type Message struct {
	Type  MessageType
	Chunk Chunk
}
