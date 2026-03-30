package scenario

import "github.com/getoutreach/plumber/example/adapter/async"

type Scenario struct {
	Publisher *async.Publisher
}

func NewScenario(publisher *async.Publisher) *Scenario {
	return &Scenario{}
}
