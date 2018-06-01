package service

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"sync"

	"github.com/chaingod/talent"

	"github.com/jadechat/meq/proto"
	"github.com/weaveworks/mesh"
)

type Node struct {
	ID       uint32
	Topic    []byte
	Subs     []Sub
	Children map[uint32]*Node
}

type SubTrie struct {
	Roots map[uint32]*Node
}

type Sub struct {
	Addr mesh.PeerName
	Cid  uint64
}

type TopicSub struct {
	Topic []byte
	Sub   Sub
}

const subCacheLen = 1000

var subCache = make(map[string]int)

var (
	wildcard = talent.MurMurHash([]byte{proto.TopicWildcard})
	sublock  = &sync.RWMutex{}
)

func NewSubTrie() *SubTrie {
	return &SubTrie{
		Roots: make(map[uint32]*Node),
	}
}

func (st *SubTrie) Subscribe(topic []byte, cid uint64, addr mesh.PeerName) error {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return err
	}
	rootid := tids[0]
	last := tids[len(tids)-1]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()

	if !ok {
		root = &Node{
			ID:       rootid,
			Children: make(map[uint32]*Node),
		}
		sublock.Lock()
		st.Roots[rootid] = root
		sublock.Unlock()
	}

	curr := root
	for _, tid := range tids[1:] {
		sublock.RLock()
		child, ok := curr.Children[tid]
		sublock.RUnlock()
		if !ok {
			child = &Node{
				ID:       tid,
				Children: make(map[uint32]*Node),
			}
			sublock.Lock()
			curr.Children[tid] = child
			sublock.Unlock()
		}

		curr = child
		// if encounters the last node in the tree branch, we should add topic to the subs of this node
		if tid == last {
			curr.Topic = topic
			sublock.Lock()
			curr.Subs = append(curr.Subs, Sub{addr, cid})
			sublock.Unlock()
		}
	}

	return nil
}

func (st *SubTrie) UnSubscribe(topic []byte, cid uint64, addr mesh.PeerName) error {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return err
	}
	rootid := tids[0]
	last := tids[len(tids)-1]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()

	if !ok {
		return errors.New("no subscribe info")
	}

	curr := root
	for _, tid := range tids[1:] {
		sublock.RLock()
		child, ok := curr.Children[tid]
		sublock.RUnlock()
		if !ok {
			return errors.New("no subscribe info")
		}

		curr = child
		// if encounters the last node in the tree branch, we should remove topic in this node
		if tid == last {
			sublock.Lock()
			for i, sub := range curr.Subs {
				if sub.Cid == cid && sub.Addr == addr {
					curr.Subs = append(curr.Subs[:i], curr.Subs[i+1:]...)
					break
				}
			}
			sublock.Unlock()
		}
	}

	return nil
}

//@todo
// add query cache for heavy lookup
func (st *SubTrie) Lookup(topic []byte) ([]TopicSub, error) {
	t := string(topic)

	tids, err := proto.ParseTopic(topic, false)
	if err != nil {
		return nil, err
	}

	var subs []TopicSub
	sublock.RLock()
	cl, ok := subCache[t]
	sublock.RUnlock()
	if ok {
		subs = make([]TopicSub, 0, cl+100)
	} else {
		subs = make([]TopicSub, 0, 10)
	}

	rootid := tids[0]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()
	if !ok {
		return nil, nil
	}

	// 所有比target长的都应该收到
	// target中的通配符'+'可以匹配任何tid
	// 找到所有路线的最后一个node节点

	var lastNodes []*Node
	if len(tids) == 1 {
		lastNodes = append(lastNodes, root)
	} else {
		st.findLastNodes(root, tids[1:], &lastNodes)
	}

	// 找到lastNode的所有子节点
	//@performance 这段代码耗时92毫秒

	sublock.RLock()
	for _, last := range lastNodes {
		st.findSubs(last, &subs)
	}
	sublock.RUnlock()

	//@todo
	//Remove duplicate elements from the list.
	if len(subs) >= subCacheLen {
		sublock.Lock()
		subCache[string(topic)] = len(subs)
		sublock.Unlock()
	}
	return subs, nil
}

func (st *SubTrie) LookupExactly(topic []byte) ([]TopicSub, error) {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return nil, err
	}

	rootid := tids[0]

	sublock.RLock()
	defer sublock.RUnlock()
	root, ok := st.Roots[rootid]
	if !ok {
		return nil, nil
	}

	// 所有比target长的都应该收到
	// target中的通配符'+'可以匹配任何tid
	// 找到所有路线的最后一个node节点
	lastNode := root
	for _, tid := range tids[1:] {
		// 任何一个node匹配不到，则认为完全无法匹配
		node, ok := lastNode.Children[tid]
		if !ok {
			return nil, nil
		}

		lastNode = node
	}

	if len(lastNode.Subs) == 0 {
		return nil, nil
	}

	var sub Sub
	// optimize the random performance
	if len(lastNode.Subs) == 1 {
		sub = lastNode.Subs[0]
	} else {
		sub = lastNode.Subs[rand.Intn(len(lastNode.Subs))]
	}

	return []TopicSub{TopicSub{lastNode.Topic, sub}}, nil
}

func (st *SubTrie) findSubs(n *Node, subs *[]TopicSub) {
	// manually realloc the slice
	if cap(*subs) == len(*subs) {
		temp := make([]TopicSub, len(*subs), cap(*subs)*6)
		copy(temp, *subs)
		*subs = temp
	}

	if len(n.Subs) > 0 {
		var sub Sub
		// optimize the random performance
		if len(n.Subs) == 1 {
			sub = n.Subs[0]
		} else {
			sub = n.Subs[rand.Intn(len(n.Subs))]
		}
		*subs = append(*subs, TopicSub{n.Topic, sub})
	}

	if len(n.Children) == 0 {
		return
	}
	for _, child := range n.Children {
		st.findSubs(child, subs)
	}
}

func (st *SubTrie) findLastNodes(n *Node, tids []uint32, nodes *[]*Node) {
	if len(tids) == 1 {
		// 如果只剩一个节点，那就直接查找，不管能否找到，都返回
		node, ok := n.Children[tids[0]]
		if ok {
			*nodes = append(*nodes, node)
		}
		return
	}

	tid := tids[0]
	if tid != wildcard {
		node, ok := n.Children[tid]
		if !ok {
			return
		}
		st.findLastNodes(node, tids[1:], nodes)
	} else {
		for _, node := range n.Children {
			st.findLastNodes(node, tids[1:], nodes)
		}
	}
}

// cluster interface

var _ mesh.GossipData = &SubTrie{}

// Encode serializes our complete state to a slice of byte-slices.
// In this simple example, we use a single gob-encoded
// buffer: see https://golang.org/pkg/encoding/gob/
func (st *SubTrie) Encode() [][]byte {
	sublock.RLock()
	defer sublock.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}
	msg := make([]byte, len(buf.Bytes())+5)
	msg[4] = CLUSTER_SUBS_SYNC_RESP
	copy(msg[5:], buf.Bytes())
	return [][]byte{msg}
}

// Merge merges the other GossipData into this one,
// and returns our resulting, complete state.
func (st *SubTrie) Merge(osubs mesh.GossipData) (complete mesh.GossipData) {
	return
}

type SubMessage struct {
	TP    int
	Topic []byte
	Cid   uint64
}

func (st SubMessage) Encode() [][]byte {
	// sync to other nodes
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}

	return [][]byte{buf.Bytes()}
}

func (st SubMessage) Merge(new mesh.GossipData) (complete mesh.GossipData) {
	return
}
