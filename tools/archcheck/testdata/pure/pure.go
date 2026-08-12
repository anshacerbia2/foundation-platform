// This file is a fixture. It lives under testdata, which the go tool ignores.
//
// It is what a domain package looks like when it conforms: transitions validated in
// memory, facts returned, nothing awaited.
package pure

import "errors"

var ErrRefused = errors.New("transition refused")

type State string

const (
	Active  State = "active"
	Revoked State = "revoked"
)

type Membership struct {
	state   State
	version int64
}

func (m *Membership) Revoke() error {
	if m.state != Active {
		return ErrRefused
	}
	m.state = Revoked
	m.version++
	return nil
}

func (m *Membership) State() State   { return m.state }
func (m *Membership) Version() int64 { return m.version }
