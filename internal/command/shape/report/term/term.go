package term

import (
	"errors"
	"fmt"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
)

// TerminalReporter is a simple implementation of contract.Reporter that outputs events to the terminal.
type TerminalReporter struct {
}

func NewTerminalReporter() *TerminalReporter {
	return &TerminalReporter{}
}

func PrintTransformer(t contract.Transformer) {
	fmt.Printf("  with annotations:\n")
	for _, annotation := range t.GetAnnotations() {
		println("   ", annotation.Name)
		println("     args:", strings.Join(annotation.Args, ", "))
		for k, v := range annotation.NamedArgs {
			println("       ", k, "=", v)
		}
	}
}

func (r *TerminalReporter) Notify(event contract.ReporterEvent) {
	switch event.Kind {
	case contract.EventTransformerAdded:
		println("Transformer added:", event.Transformer.GetName())
		PrintTransformer(event.Transformer)
	case contract.EventTransformerSkipped:
		println("Transformer skipped:", event.Transformer.GetName(), "-", event.Message)
	case contract.EventTransformerError:
		var syntaxErr *contract.SyntaxError
		if errors.As(event.Error, &syntaxErr) {
			println("Transformer error in", event.Transformer.GetName(), ":")
			fmt.Println(syntaxErr.String())
		} else {
			println("Transformer error in", event.Transformer.GetName(), ":", event.Error.Error())
		}
	case contract.EventTransformerOutput:
		println("Transformer output from", event.Transformer.GetName(), ":", event.Path)
	case contract.EventTransformerInfo:
		println("Info from", event.Transformer.GetName(), ":", event.Message)
	case contract.EventTransformerRestored:
		if event.Error != nil {
			println("Restored output with error:", event.Path, "-", event.Error.Error())
		} else {
			println("Restored output:", event.Path)
		}
	case contract.EventQueryExecuted:
		println("Query executed:", event.Message)
	case contract.EventQueryError:
		println("Query error:", event.Error.Error())
	default:
		println("Unknown event type:", string(event.Kind))
	}
}
