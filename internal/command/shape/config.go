package shape

import "github.com/getoutreach/plumber/internal/command/inspect"

type Config struct {
	Shape   ShapeConfig    `yaml:"plumber.shape"`
	Inspect inspect.Config `yaml:"plumber.inspect"`
}

type ShapeConfig struct {
	Templates []TemplateConfig `yaml:"templates,omitempty"`
	Mixins    []MixinConfig    `yaml:"mixins,omitempty"`
}

type MixinConfig struct {
	PlumberMixin *PlumberMixinConfig `yaml:"plumber.mixin,omitempty"`
}

type PlumberMixinConfig struct {
	Name        string             `yaml:"name"`
	Annotations []AnnotationConfig `yaml:"annotations,omitempty"`
}

type AnnotationConfig struct {
	Name      string            `yaml:"name"`
	Args      []string          `yaml:"args,omitempty"`
	NamedArgs map[string]string `yaml:"namedArgs,omitempty"`
}

type TemplateConfig struct {
	PlumberTemplate *PlumberTemplateConfig `yaml:"plumber.template,omitempty"`
}

type PlumberTemplateConfig struct {
	Name     string  `yaml:"name"`
	Content  *string `yaml:"content"`
	FileName *string `yaml:"filename,omitempty"`
}
