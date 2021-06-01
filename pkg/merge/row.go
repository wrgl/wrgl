package merge

type Rows struct {
	Values [][]string
	Layers []int
}

func NewRows(n int) *Rows {
	return &Rows{
		Values: make([][]string, n),
		Layers: make([]int, n),
	}
}

func (r *Rows) Reset() {
	r.Layers = r.Layers[:0]
	r.Values = r.Values[:0]
}

func (r *Rows) Append(layer int, row []string) {
	r.Layers = append(r.Layers, layer)
	r.Values = append(r.Values, row)
}

func (r *Rows) Len() int {
	return len(r.Values)
}

func (r *Rows) Less(i, j int) bool {
	return r.Layers[i] < r.Layers[j]
}

func (r *Rows) Swap(i, j int) {
	r.Values[i], r.Values[j] = r.Values[j], r.Values[i]
	r.Layers[i], r.Layers[j] = r.Layers[j], r.Layers[i]
}
