package replication

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const defaultTimeout = 10 * time.Second

// State is a wrapper around the persistent key-value storage
// used to store information about the replication state.
type State struct {
	logger *log.Logger
	cl     *clientv3.Client
	prefix string
}

// NewState initialises the connection to the etcd cluster.
func NewState(logger *log.Logger, addr []string, clusterName string) (*State, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: defaultTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etcd client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err = etcdClient.Put(ctx, "test", "test")
	if err != nil {
		return nil, fmt.Errorf("could not set the test key: %w", err)
	}

	return &State{
		logger: logger,
		cl:     etcdClient,
		prefix: "chukcha/" + clusterName + "/",
	}, nil
}

func (c *State) put(ctx context.Context, key, value string) error {
	_, err := c.cl.Put(ctx, c.prefix+key, value)
	return err
}

type Result struct {
	Key   string
	Value string
}

type Option clientv3.OpOption

func WithPrefix() Option { return Option(clientv3.WithPrefix()) }

func (c *State) get(ctx context.Context, key string, opts ...Option) ([]Result, error) {
	etcdOpts := make([]clientv3.OpOption, 0, len(opts))
	for _, o := range opts {
		etcdOpts = append(etcdOpts, clientv3.OpOption(o))
	}

	etcdRes, err := c.cl.Get(ctx, c.prefix+key, etcdOpts...)
	if err != nil {
		return nil, err
	}

	res := make([]Result, 0, len(etcdRes.Kvs))
	for _, kv := range etcdRes.Kvs {
		res = append(res, Result{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}

	return res, nil
}

type Peer struct {
	InstanceName string
	ListenAddr   string
}

func (c *State) RegisterNewPeer(ctx context.Context, p Peer) error {
	return c.put(ctx, "peers/"+p.InstanceName, p.ListenAddr)
}

func (c *State) ListPeers(ctx context.Context) ([]Peer, error) {
	resp, err := c.get(ctx, "peers/", WithPrefix())
	if err != nil {
		return nil, err
	}

	res := make([]Peer, 0, len(resp))
	for _, kv := range resp {
		res = append(res, Peer{
			InstanceName: strings.TrimPrefix(kv.Key, c.prefix+"peers/"),
			ListenAddr:   kv.Value,
		})
	}

	return res, nil
}

type Chunk struct {
	Owner    string
	Category string
	FileName string
}

func (c *State) AddChunkToReplicationQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "replication/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	return c.put(ctx, key, ch.Owner)
}

func (c *State) AddChunkToAcknowledgeQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "acknowledge/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	return c.put(ctx, key, ch.Owner)
}

func (c *State) DeleteChunkFromReplicationQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "replication/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	_, err := c.cl.Delete(ctx, c.prefix+key)
	return err
}

func (c *State) DeleteChunkFromAcknowledgeQueue(ctx context.Context, targetInstance string, ch Chunk) error {
	key := "acknowledge/" + targetInstance + "/" + ch.Category + "/" + ch.FileName
	_, err := c.cl.Delete(ctx, c.prefix+key)
	return err
}

func (c *State) parseReplicationKey(prefix string, kv *mvccpb.KeyValue) (Chunk, error) {
	parts := strings.SplitN(strings.TrimPrefix(string(kv.Key), prefix), "/", 2)
	if len(parts) != 2 {
		return Chunk{}, fmt.Errorf("unexpected key %q, expected two parts after prefix %q", string(kv.Key), prefix)
	}

	return Chunk{
		Owner:    string(kv.Value),
		Category: parts[0],
		FileName: parts[1],
	}, nil
}

// WatchReplicationQueue starts watching the replication queue and returns
// all the existing chunks too.
func (c *State) WatchReplicationQueue(ctx context.Context, instanceName string) chan Chunk {
	return c.watchQueue(ctx, "replication", instanceName)
}

// WatchAcknowledgeQueue starts watching the ack queue and returns
// all the existing chunks too.
func (c *State) WatchAcknowledgeQueue(ctx context.Context, instanceName string) chan Chunk {
	return c.watchQueue(ctx, "acknowledge", instanceName)
}

func (c *State) watchQueue(ctx context.Context, queueName string, instanceName string) chan Chunk {
	prefix := c.prefix + queueName + "/" + instanceName + "/"
	resCh := make(chan Chunk)

	go func() {
		resp, err := c.cl.Get(ctx, prefix, clientv3.WithPrefix())
		// TODO: handle errors better.
		if err != nil {
			c.logger.Printf("etcd list keys failed (SOME CHUNKS WILL NOT BE DOWNLOADED): %v", err)
			return
		}

		for _, kv := range resp.Kvs {
			ch, err := c.parseReplicationKey(prefix, kv)
			if err != nil {
				c.logger.Printf("etcd initial key list error: %v", err)
				continue
			}

			resCh <- ch
		}
	}()

	go func() {
		for resp := range c.cl.Watch(clientv3.WithRequireLeader(ctx), prefix, clientv3.WithPrefix()) {
			// TODO: handle errors better.
			if err := resp.Err(); err != nil {
				c.logger.Printf("etcd watch error: %v", err)
				return
			}

			for _, ev := range resp.Events {
				if len(ev.Kv.Value) == 0 {
					continue
				}

				ch, err := c.parseReplicationKey(prefix, ev.Kv)
				if err != nil {
					c.logger.Printf("etcd watch error: %v", err)
					continue
				}

				resCh <- ch
			}
		}
	}()

	return resCh
}
