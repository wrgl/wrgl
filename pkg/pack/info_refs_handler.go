package pack

import (
	"encoding/hex"
	"net/http"
	"sort"
	"strings"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/versioning"
)

type InfoRefsHandler struct {
	db kv.DB
}

func NewInfoRefsHandler(db kv.DB) *InfoRefsHandler {
	return &InfoRefsHandler{db: db}
}

func (h *InfoRefsHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	refs, err := versioning.ListLocalRefs(h.db)
	if err != nil {
		panic(err)
	}
	pairs := make([][]string, 0, len(refs))
	for k, v := range refs {
		pairs = append(pairs, []string{
			hex.EncodeToString(v), k,
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][1] < pairs[j][1]
	})
	rw.Header().Add("Content-Type", "application/x-wrgl-upload-pack-advertisement")
	buf := misc.NewBuffer(nil)
	for _, sl := range pairs {
		err := encoding.WritePktLine(rw, buf, strings.Join(sl, " "))
		if err != nil {
			panic(err)
		}
	}
	err = encoding.WritePktLine(rw, buf, "")
	if err != nil {
		panic(err)
	}
}
