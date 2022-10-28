package main

import "github.com/ccbhj/raft_lab/cli"

func main() {
	execCtx := cli.NewExecContext()
	defer func() {
		if execCtx != nil {
			execCtx.Close()
		}
	}()
	execCtx.Prompt()
}
