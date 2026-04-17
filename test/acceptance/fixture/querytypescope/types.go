package querytypescope

// Registry holds named getter functions for testing type-scoped queries.
type Registry struct {
	GetAlpha func() string
	GetBeta  func() string
	SetGamma func(string)
}
