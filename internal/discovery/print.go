// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Print functions for configuration graph visualization
// Managed: true

package discovery

import (
	"fmt"
	"io"
	"strings"
)

// PrintConfig prints the loaded configuration graph in a readable format
func PrintConfig(w io.Writer, cfg *Config) {
	fmt.Fprintf(w, "Configuration Graph\n")
	fmt.Fprintf(w, "===================\n\n")

	for i, app := range cfg.Applications {
		fmt.Fprintf(w, "Application %d: %s\n", i+1, app.Name)
		fmt.Fprintf(w, "└─ Containers: %d\n", len(app.Containers))

		for j, container := range app.Containers {
			printContainer(w, j+1, &container, "   ")
		}

		fmt.Fprintf(w, "\n")
	}
}

func printContainer(w io.Writer, idx int, container *Container, indent string) {
	isLast := false // Could be enhanced to detect last item
	prefix := "├─"
	if isLast {
		prefix = "└─"
	}

	if container.PlumberContainer != nil {
		cfg := container.PlumberContainer
		fmt.Fprintf(w, "%s%s Container %d: %s\n", indent, prefix, idx, cfg.Name)

		subIndent := indent + "│  "
		if isLast {
			subIndent = indent + "   "
		}

		if cfg.Comment != "" {
			fmt.Fprintf(w, "%s├─ Comment: %s\n", subIndent, cfg.Comment)
		}

		fmt.Fprintf(w, "%s├─ Path: %s\n", subIndent, cfg.Container.Path)

		if len(cfg.Matchers) > 0 {
			fmt.Fprintf(w, "%s├─ Matchers: %d\n", subIndent, len(cfg.Matchers))
			for k, matcher := range cfg.Matchers {
				printMatcher(w, k+1, &matcher, subIndent+"│  ")
			}
		}
	}

	if container.Loop != nil {
		fmt.Fprintf(w, "%s└─ Loop:\n", indent)
		fmt.Fprintf(w, "%s   └─ Path pattern: %s\n", indent, container.Loop.Path)
	}
}

func printMatcher(w io.Writer, idx int, matcher *Matcher, indent string) {
	if matcher.PlumberMatcherStruct != nil {
		cfg := matcher.PlumberMatcherStruct
		fmt.Fprintf(w, "%s├─ Matcher %d: struct\n", indent, idx)

		if len(cfg.Constructors) > 0 {
			fmt.Fprintf(w, "%s│  └─ Constructors: %s\n", indent,
				strings.Join(cfg.Constructors, ", "))
		}
	}
}

// PrintHydratedConfig prints the configuration after loop hydration
func PrintHydratedConfig(w io.Writer, hydrated []*PlumberContainerConfig) {
	fmt.Fprintf(w, "Hydrated Configuration\n")
	fmt.Fprintf(w, "======================\n\n")

	for i, cfg := range hydrated {
		fmt.Fprintf(w, "Container %d:\n", i+1)
		fmt.Fprintf(w, "  Name: %s\n", cfg.Name)
		if cfg.Comment != "" {
			fmt.Fprintf(w, "  Comment: %s\n", cfg.Comment)
		}
		fmt.Fprintf(w, "  Path: %s\n", cfg.Container.Path)

		if len(cfg.Matchers) > 0 {
			fmt.Fprintf(w, "  Matchers:\n")
			for j, matcher := range cfg.Matchers {
				if matcher.PlumberMatcherStruct != nil {
					fmt.Fprintf(w, "    %d. Struct matcher\n", j+1)
					if len(matcher.PlumberMatcherStruct.Constructors) > 0 {
						fmt.Fprintf(w, "       Constructors: %s\n",
							strings.Join(matcher.PlumberMatcherStruct.Constructors, ", "))
					}
				}
			}
		}

		fmt.Fprintf(w, "\n")
	}
}
