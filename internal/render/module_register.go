// Copyright 2024 Outreach Corporation. All Rights Reserved.

// Description: Module registry for tracking imported modules during rendering.

package render

import (
	"fmt"
	"path"
	"strconv"

	"github.com/samber/lo"
)

// ModuleRegistration represents a registered module with its name, import ID, and a counter for handling
// multiple imports of the same package.
type ModuleRegistration struct {
	Name    string
	ID      string
	Counter int
}

func (r ModuleRegistration) IsStd() bool {
	return r.ID == r.Name
}

func (r ModuleRegistration) IsPrimary() bool {
	return r.ID == path.Base(r.Name)
}

func (r *ModuleRegistration) Inc() int {
	r.Counter++
	return r.Counter
}

func (r ModuleRegistration) String() string {
	if r.IsStd() || r.IsPrimary() {
		return fmt.Sprintf("%q", r.Name)
	}
	return fmt.Sprintf("%s %q", r.ID, r.Name)
}

// ModuleRegister is a registry for tracking imported modules during the rendering process, ensuring that necessary imports
// are included in the generated code.
type ModuleRegister struct {
	presence map[string]ModuleRegistration
	Imports  []ModuleRegistration
}

func NewModuleRegister() *ModuleRegister {
	return &ModuleRegister{
		presence: make(map[string]ModuleRegistration),
	}
}

func (r *ModuleRegister) Registrations() map[string]ModuleRegistration {
	return r.presence
}

func (r *ModuleRegister) Dot(name string) ModuleRegistration {
	r.presence[name] = ModuleRegistration{Name: name, ID: "."}
	r.Imports = append(r.Imports, r.presence[name])
	return r.presence[name]
}

func (r *ModuleRegister) Register(name string, std bool) ModuleRegistration {
	if _, exists := r.presence[name]; !exists {
		id := name
		if !std {
			packageName := path.Base(name)
			reg, alreadyExists := lo.Find(r.Imports, func(reg ModuleRegistration) bool {
				return packageName == reg.ID
			})
			if alreadyExists {
				id = packageName + "_" + strconv.Itoa(reg.Inc())
			} else {
				id = packageName
			}
		}
		reg := ModuleRegistration{Name: name, ID: id}
		r.presence[name] = reg
		r.Imports = append(r.Imports, reg)
		return reg
	}
	return r.presence[name]
}
