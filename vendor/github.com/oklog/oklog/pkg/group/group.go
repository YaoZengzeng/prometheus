// Package group implements an actor-runner with deterministic teardown. It is
// somewhat similar to package errgroup, except it does not require actor
// goroutines to understand context semantics. This makes it suitable for use
// in more circumstances; for example, goroutines which are handling
// connections from net.Listeners, or scanning input from a closable io.Reader.
package group

// Group collects actors (functions) and runs them concurrently.
// When one actor (function) returns, all actors are interrupted.
// The zero value of a Group is useful.
// Group收集actors并且并发地运行它们
// 当一个actor返回，所有的actors都被中断
// 一个Group的零值是有用的
type Group struct {
	actors []actor
}

// Add an actor (function) to the group. Each actor must be pre-emptable by an
// interrupt function. That is, if interrupt is invoked, execute should return.
// Also, it must be safe to call interrupt even after execute has returned.
// Add向group增加一个actor，每个actor必须有一个interrupt function，这意味着如果发生了interrupt
// 执行需要返回，同时即使execute已经返回，调用interrupt也必须是安全的
//
// The first actor (function) to return interrupts all running actors.
// The error is passed to the interrupt functions, and is returned by Run.
// 第一个actor的返回会中断所有正在运行的actors
func (g *Group) Add(execute func() error, interrupt func(error)) {
	g.actors = append(g.actors, actor{execute, interrupt})
}

// Run all actors (functions) concurrently.
// When the first actor returns, all others are interrupted.
// Run only returns when all actors have exited.
// Run returns the error returned by the first exiting actor.
func (g *Group) Run() error {
	if len(g.actors) == 0 {
		return nil
	}

	// Run each actor.
	errors := make(chan error, len(g.actors))
	for _, a := range g.actors {
		go func(a actor) {
			errors <- a.execute()
		}(a)
	}

	// Wait for the first actor to stop.
	err := <-errors

	// Signal all actors to stop.
	for _, a := range g.actors {
		a.interrupt(err)
	}

	// Wait for all actors to stop.
	for i := 1; i < cap(errors); i++ {
		<-errors
	}

	// Return the original error.
	return err
}

type actor struct {
	execute   func() error
	interrupt func(error)
}
