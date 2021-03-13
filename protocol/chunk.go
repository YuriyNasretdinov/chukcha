package protocol

// Chunk is a piece of data that contains the messages that were written to it.
// It can be incomplete which means that it currently being written into.
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
	Size     uint64 `json:"size"`
}
