// Package errgroup is a signature-only stand-in for golang.org/x/sync/errgroup.
package errgroup

import "context"

type Group struct{}

func WithContext(ctx context.Context) (*Group, context.Context) { return &Group{}, ctx }

func (g *Group) Go(f func() error) {}

func (g *Group) TryGo(f func() error) bool { return true }

func (g *Group) Wait() error { return nil }

func (g *Group) SetLimit(n int) {}
