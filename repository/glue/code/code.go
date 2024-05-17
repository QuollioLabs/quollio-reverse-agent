package code

import "fmt"

const (
	NOT_AUTHORIZED     = "NOT_AUTHORIZED"
	RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
)

type GlueError struct {
	Number      int
	ErrorReason string
	Message     string
	Err         error
}

func (g *GlueError) Error() string {
	return fmt.Sprintf("%d: %s", g.Number, g.Message)
}

func (g *GlueError) Unwrap() error {
	return g.Err
}
