package querylocal

// Setup initializes all subsystems by querying for Init functions.
func Setup() {
	// plumber:query "^Init.*" scope="."
	var initFuncs = []func(){}
	for _, f := range initFuncs {
		f()
	}
}
