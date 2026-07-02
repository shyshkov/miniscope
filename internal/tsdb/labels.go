package tsdb

import (
	"sort"
	"strings"
)

// Labels identify a time series, e.g. {__name__="http_requests_total",
// method="GET", status="200"}. They are kept sorted by name so a series has a
// single canonical string form usable as a map key (its "series ID").
type Labels []Label

type Label struct {
	Name, Value string
}

func NewLabels(kv map[string]string) Labels {
	ls := make(Labels, 0, len(kv))
	for k, v := range kv {
		ls = append(ls, Label{k, v})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// String is the canonical, stable identity of the series.
func (ls Labels) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

// Matcher is a single label predicate, the building block of a selector like
// http_requests_total{method="GET"}.
type Matcher struct {
	Name, Value string
}

func (m Matcher) Matches(ls Labels) bool { return ls.Get(m.Name) == m.Value }

// Selector is a conjunction of matchers (all must match).
type Selector []Matcher

func (s Selector) Matches(ls Labels) bool {
	for _, m := range s {
		if !m.Matches(ls) {
			return false
		}
	}
	return true
}
