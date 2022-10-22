package doctor

import "github.com/wrgl/wrgl/pkg/objects"

type commitMap map[string][]byte

func (m commitMap) update(oldSum, newSum []byte) {
	m[string(oldSum)] = newSum
}

func (m commitMap) parentsUpdated(com *objects.Commit) bool {
	updated := false
	for i, sum := range com.Parents {
		if b, ok := m[string(sum)]; ok {
			updated = true
			com.Parents[i] = b
		}
	}
	return updated
}
