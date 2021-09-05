package protocol

import (
	"regexp"
	"strconv"
	"strings"
)

var filenameRegexp = regexp.MustCompile("^chunk([0-9]+)$")

// Chunk is a piece of data that contains the messages that were written to it.
// It can be incomplete which means that it currently being written into.
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
	Size     uint64 `json:"size"`
}

func ParseChunkFileName(filename string) (instance string, chunkIdx int) {
	idx := strings.LastIndexByte(filename, '-')
	if idx < 0 {
		return "", 0
	}

	instance = filename[0:idx]
	chunkName := filename[idx+1:]

	var err error

	res := filenameRegexp.FindStringSubmatch(chunkName)
	if res == nil {
		return "", 0
	}

	chunkIdx, err = strconv.Atoi(res[1])
	if err != nil {
		return "", 0
	}

	return instance, chunkIdx
}
