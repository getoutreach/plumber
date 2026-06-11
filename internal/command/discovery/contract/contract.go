// Copyright 2026 Outreach Corporation. All Rights Reserved.

// Description: This file defines core contract types for the discovery system including Provider,
// ConstructorInfo, ParameterInfo, and ProviderMapping.

// Package contract defines shared types for the plumber discovery system including providers, constructors, and parameter information.
package contract

import (
	"go/types"

	"github.com/dave/dst/decorator"
)

// ProviderMapping represents a mapping of a provider type to the container
// providers that can create it, used for tracking discovered providers and their
// associated constructors during the discovery process.
type ProviderMapping struct {
	Type      types.Type
	Providers []*ContainerProvider
}

// ContainerProvider represents a provider extracted from constructor patterns,
// including the provider name and the associated constructor information.
type ContainerProvider struct {
	ContainerName string
	Provider      *Provider
}

// DiscoveryResult contains the discovered providers
type DiscoveryResult struct {
	Providers []*Provider
}

// Provider represents a provider extracted from constructor patterns
// Provider is the top-level entity that can have multiple constructors creating it
type Provider struct {
	Name        string           // Provider name extracted from named capture group
	Type        *ParameterInfo   // Type of the provider (from constructor return type)
	Constructor *ConstructorInfo // Constructor functions that create this provider
}

// ConstructorInfo contains information about a constructor function
type ConstructorInfo struct {
	FunctionName     string // Original function name
	ReturnType       *ParameterInfo
	Parameters       Parameters
	ReturnParameters Parameters
	Comment          string
	File             string
	Package          string
}

// Parameters is a slice of ParameterInfo
type Parameters []*ParameterInfo

// ParameterInfo contains information about a function parameters or results
type ParameterInfo struct {
	Name     string
	TypeName string
	TypeInfo *TypeInfo
}

// TypeInfo contains information about a type, including its package and the type itself.
type TypeInfo struct {
	Package *decorator.Package
	Type    types.Type
}

func (p ParameterInfo) IsError() bool {
	return p.TypeInfo.Type.String() == "error"
}

func (c *ConstructorInfo) ReturnsError() bool {
	if len(c.ReturnParameters) == 0 {
		return false
	}
	return c.ReturnParameters[len(c.ReturnParameters)-1].IsError()
}
