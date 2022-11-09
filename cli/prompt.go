package cli

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"

	"github.com/ccbhj/raft_lab/logging"
	"github.com/pkg/errors"
)

func (c *ExecContext) Prompt() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		exit(c, nil)
	}()

	name := "router"
	if c.rf != nil {
		name = c.rf.Name()
	}
	logging.FprintfColor(os.Stdout, logging.ColorGRN,
		"welcome to raft_lab, %s\n", name)
	c.ListCommand()

	reader := bufio.NewReader(os.Stdin)
	for {
		print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fatal(err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		s := strings.Split(line, " ")
		cmdName, args := s[0], s[1:]
		if cmdName == "h" || cmdName == "help" {
			c.ListCommand()
			continue
		}
		cmd, in := c.commands[cmdName]
		if !in {
			printError(errors.Errorf("unknown command %s", cmdName))
			continue
		}

		err = func() (e error) {
			defer func() {
				if r := recover(); r != nil {
					debug.PrintStack()
					e = errors.Errorf("%v", r)
				}
			}()
			if err := cmd.Handler(c, args); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			printError(err)
		}
	}
}

func fatal(e error) {
	fmt.Fprintln(os.Stderr, logging.SprintfColor(logging.ColorRED, "got error: %s", e))
	os.Exit(1)
}

func printError(e error) {
	fmt.Fprintln(os.Stderr, logging.SprintfColor(logging.ColorRED, "%s", e))
}
