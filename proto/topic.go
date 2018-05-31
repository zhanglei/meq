package proto

import (
	"errors"

	"github.com/chaingod/talent"
)

const (
	TopicSep      = '/'
	TopicWildcard = '+'
	TopicQueueSep = '?'
)

func ParseTopic(t []byte, exactly bool) ([]uint32, error) {
	var tids []uint32
	var err error

	if len(t) == 0 {
		err = errors.New("topic cant be empty")
		return nil, err
	}

	var buf []byte
	for i, b := range t {
		if i == 0 { // first byte must be topic sep
			if b != TopicSep {
				err = errors.New("topic invalid")
				return nil, err
			}
			continue
		}
		if i == len(t)-1 { // last byte must not be topic sep
			if b == TopicSep {
				err = errors.New("topic invalid")
				return nil, err
			}
		}
		if b != TopicSep {
			buf = append(buf, b)
			continue
		}

		if len(buf) == 0 {
			err = errors.New("topic invalid")
			return nil, err
		}
		tid := talent.MurMurHash(buf)
		tids = append(tids, tid)
		buf = buf[:0]
	}

	if len(buf) != 0 {
		tid := talent.MurMurHash(buf)
		tids = append(tids, tid)
	}

	if len(tids) == 0 {
		return nil, errors.New("topic invalid")
	}
	// first part of topic cant be wildcard
	if tids[0] == talent.MurMurHash([]byte{TopicWildcard}) {
		return nil, errors.New("first byte cant be wildcard")
	}

	if exactly {
		if len(tids) < 3 {
			return nil, errors.New("topic invalid")
		}
		// if the topic is for subscribe,every part of topic cant be wildcard
		for _, tid := range tids[1:] {
			if tid == talent.MurMurHash([]byte{TopicWildcard}) {
				return nil, errors.New("subscribe topic  cant contain wildcard")
			}
		}

	} else {
		if len(tids) < 1 {
			return nil, errors.New("topic invalid")
		}
		// last part cant be wildcard
		if tids[len(tids)-1] == talent.MurMurHash([]byte{TopicWildcard}) {
			return nil, errors.New("last part cant be wildcart in publish mode")
		}
	}
	return tids, nil
}

func GetTopicAndQueue(t []byte) ([]byte, []byte) {
	if t[len(t)-1] == TopicQueueSep {
		return t, DEFAULT_QUEUE
	}

	for i, b := range t {
		if b == TopicQueueSep {
			return t[:i], t[i+1:]
		}
	}

	return t, DEFAULT_QUEUE
}
