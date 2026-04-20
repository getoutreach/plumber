package mergecomplex

// plumber:derive ServiceBlended
// plumber:mode inplace
// plumber:template mergecomplex-override
type ServiceModel struct {
	Logger Logger
	DB     Database
	Cache  Cache
}
