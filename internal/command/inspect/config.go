package inspect

type Config struct {
	Inspect *InspectConfig `yaml:"plumber.inspect"`
}

type InspectConfig struct {
	Format            string              `yaml:"format,omitempty"`
	AnnotationsConfig []AnnotationsConfig `yaml:"annotations,omitempty"`
}

type AnnotationsConfig struct {
	List []string `yaml:"list,omitempty"`
}
