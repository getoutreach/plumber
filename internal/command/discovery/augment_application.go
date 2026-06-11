// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: AST augmentation for the root application container, ensuring all
// sub-containers are declared as struct fields, initialized with new(), and passed
// to DefineContainers.

package discovery

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/getoutreach/plumber/internal/astx"
)

// ApplicationAugmentResult contains information about the application augmentation.
type ApplicationAugmentResult struct {
	Added   []string // Container names that were added
	Skipped []string // Container names already present
}

// AugmentApplicationStruct ensures the root application Container struct declares
// all sub-containers as pointer fields, initializes them with new() in the
// NewApplication composite literal, and passes them to DefineContainers.
func (a *Augmenter) AugmentApplicationStruct(
	pkg *decorator.Package,
	applicationPath string,
	containerNames []string,
	file *dst.File,
	dec *decorator.Decorator,
) (*ApplicationAugmentResult, error) {
	// Find the Container struct (root application container)
	structDecl := a.findStructDecl(file, "Container")
	if structDecl == nil {
		return nil, fmt.Errorf("struct %q not found in file %s", "Container", applicationPath)
	}

	result := &ApplicationAugmentResult{}

	// Determine which containers are missing from the struct
	existingFields := make(map[string]bool)
	if structDecl.Fields != nil {
		for _, field := range structDecl.Fields.List {
			for _, name := range field.Names {
				existingFields[name.Name] = true
			}
		}
	}

	var missing []string
	for _, name := range containerNames {
		if existingFields[name] {
			result.Skipped = append(result.Skipped, name)
		} else {
			missing = append(missing, name)
		}
	}

	if len(missing) == 0 {
		return result, nil
	}

	// Add missing fields to struct as *ContainerType
	for _, name := range missing {
		field := &dst.Field{
			Names: []*dst.Ident{{Name: name}},
			Type:  &dst.StarExpr{X: &dst.Ident{Name: name}},
		}
		field.Decs.Before = dst.NewLine
		structDecl.Fields.List = append(structDecl.Fields.List, field)
		result.Added = append(result.Added, name)
	}

	// Find NewApplication function to add initialization and DefineContainers args
	newAppFunc := astx.FuncDeclaration(file, "NewApplication")
	if newAppFunc == nil {
		// Write struct changes even if we can't find NewApplication
		if err := a.writeFile(applicationPath, file, dec); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
		return result, nil
	}

	// Find the composite literal &Container{...} in the function body
	compLit := a.findContainerCompositeLit(newAppFunc)
	if compLit != nil {
		// Add missing key-value pairs: ContainerName: new(ContainerType)
		existingKeys := make(map[string]bool)
		for _, elt := range compLit.Elts {
			if kv, ok := elt.(*dst.KeyValueExpr); ok {
				if ident, ok := kv.Key.(*dst.Ident); ok {
					existingKeys[ident.Name] = true
				}
			}
		}

		for _, name := range missing {
			if existingKeys[name] {
				continue
			}
			kv := &dst.KeyValueExpr{
				Key: &dst.Ident{Name: name},
				Value: &dst.CallExpr{
					Fun:  &dst.Ident{Name: "new"},
					Args: []dst.Expr{&dst.Ident{Name: name}},
				},
			}
			kv.Decs.Before = dst.NewLine
			kv.Decs.After = dst.NewLine
			compLit.Elts = append(compLit.Elts, kv)
		}
	}

	// Find the DefineContainers call and add missing a.ContainerName args
	a.addDefineContainersArgs(newAppFunc, missing)

	// Write back
	if err := a.writeFile(applicationPath, file, dec); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	return result, nil
}

// findContainerCompositeLit finds the &Container{...} composite literal in a
// function body (the first unary &expr with a composite lit of type Container).
func (a *Augmenter) findContainerCompositeLit(
	funcDecl *dst.FuncDecl,
) *dst.CompositeLit {
	node := astx.FindNode(funcDecl, func(n dst.Node) (match, recurse bool) {
		if unary, ok := n.(*dst.UnaryExpr); ok {
			if cl, ok := unary.X.(*dst.CompositeLit); ok {
				if ident, ok := cl.Type.(*dst.Ident); ok {
					if ident.Name == "Container" {
						return true, false
					}
				}
			}
		}
		return false, true
	})
	if node == nil {
		return nil
	}
	return node.(*dst.UnaryExpr).X.(*dst.CompositeLit)
}

// addDefineContainersArgs finds the DefineContainers(...) call in the function
// and adds missing a.ContainerName arguments.
func (a *Augmenter) addDefineContainersArgs(
	funcDecl *dst.FuncDecl,
	missing []string,
) {
	node := astx.FindNode(funcDecl, func(n dst.Node) (match, recurse bool) {
		if call, ok := n.(*dst.CallExpr); ok {
			// DST represents imported package calls as Ident with Path
			if ident, ok := call.Fun.(*dst.Ident); ok {
				if ident.Name == "DefineContainers" {
					return true, false
				}
			}
			// Also handle SelectorExpr form (e.g., plumber.DefineContainers)
			if sel, ok := call.Fun.(*dst.SelectorExpr); ok {
				if sel.Sel.Name == "DefineContainers" {
					return true, false
				}
			}
		}
		return false, true
	})
	if node == nil {
		return
	}

	call := node.(*dst.CallExpr)

	// Build set of existing a.X arguments
	existingArgs := make(map[string]bool)
	for _, arg := range call.Args {
		if sel, ok := arg.(*dst.SelectorExpr); ok {
			if x, ok := sel.X.(*dst.Ident); ok && x.Name == "a" {
				existingArgs[sel.Sel.Name] = true
			}
		}
	}

	for _, name := range missing {
		if existingArgs[name] {
			continue
		}
		arg := &dst.SelectorExpr{
			X:   &dst.Ident{Name: "a"},
			Sel: &dst.Ident{Name: name},
		}
		arg.Decs.Before = dst.NewLine
		arg.Decs.After = dst.NewLine
		call.Args = append(call.Args, arg)
	}
}
