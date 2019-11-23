package segdb

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
)

// Segment struct
type Segment struct {
	ID      string
	Data    string
	Filters string
	Indexes map[string]interface{}
	Program *vm.Program
}

// Match with map
func (s *Segment) Match(m map[string]interface{}) bool {
	output, err := expr.Run(s.Program, m)
	if err != nil {
		return false
	}

	b, ok := output.(bool)
	return ok == true && b == true
}
