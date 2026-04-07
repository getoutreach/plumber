package shape

import "github.com/getoutreach/plumber/internal/command/inspect"

type Config struct {
	Shape    ShapeConfig     `yaml:"plumber.shape"`
	Inspect  inspect.Config  `yaml:"plumber.inspect"`
	Includes []IncludeConfig `yaml:"includes,omitempty"`
}

type ShapeConfig struct {
	WorkingDir string           `yaml:"workingDir,omitempty"`
	Templates  []TemplateConfig `yaml:"templates,omitempty"`
	Mixins     []MixinConfig    `yaml:"mixins,omitempty"`
	Type       TypeConfig       `yaml:"type,omitempty"`
}

type TypeConfig struct {
	Wrappers []WrapperConfig `yaml:"wrappers,omitempty"`
}

type WrapperConfig struct {
	PlumberWrapper *PlumberWrapperConfig `yaml:"plumber.wrapper,omitempty"`
}

type PlumberWrapperConfig struct {
	Name        string                    `yaml:"name"`
	Expressions []WrapperExpressionConfig `yaml:"expressions,omitempty"`
}

type WrapperExpressionConfig struct {
	PlumberWrapperExpression *PlumberWrapperExpressionConfig `yaml:"plumber.wrapper_expression,omitempty"`
}

type PlumberWrapperExpressionConfig struct {
	Type    string            `yaml:"type"`
	Matches []MatchRuleConfig `yaml:"matches,omitempty"`
}

type MatchRuleConfig struct {
	Rule string `yaml:"rule"`
}

type IncludeConfig struct {
	Path string `yaml:"path"`
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

func (c *Config) Merge(includes ...*Config) {
	for _, include := range includes {
		c.Shape.Templates = append(c.Shape.Templates, include.Shape.Templates...)
		c.Shape.Mixins = append(c.Shape.Mixins, include.Shape.Mixins...)
		c.Shape.Type.Wrappers = append(c.Shape.Type.Wrappers, include.Shape.Type.Wrappers...)
	}
}
