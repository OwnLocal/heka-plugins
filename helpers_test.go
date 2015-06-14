package ol_heka_test

import "github.com/mozilla-services/heka/message"

func newField(name string, value interface{}, representation string) *message.Field {
	field, err := message.NewField(name, value, representation)
	if err != nil {
		panic(err)
	}
	return field
}

type fields []*message.Field

// Implement sort.Interface to make fields sortable.
func (f fields) Len() int           { return len(f) }
func (f fields) Less(i, j int) bool { return *(f[i].Name) < *(f[j].Name) }
func (f fields) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
