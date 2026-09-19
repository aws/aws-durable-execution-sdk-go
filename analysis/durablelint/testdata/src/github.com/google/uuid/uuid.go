// Package uuid is a signature-only stand-in for github.com/google/uuid.
package uuid

type UUID [16]byte

func New() UUID                    { return UUID{} }
func NewString() string            { return "" }
func NewRandom() (UUID, error)     { return UUID{}, nil }
func Parse(s string) (UUID, error) { return UUID{}, nil }
