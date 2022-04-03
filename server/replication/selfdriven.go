package replication

type Peer struct {
	InstanceName string
	ListenAddr   string
}

type Chunk struct {
	Owner    string
	Category string
	FileName string
}
