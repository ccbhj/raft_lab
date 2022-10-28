package cli

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/ccbhj/raft_lab/raft"
	"github.com/pkg/errors"
)

type Command struct {
	Usage   string
	Handler func(ctx *ExecContext, args []string) error
}

var commonCommands = map[string]Command{
	"exit": {
		Usage:   "exit the cli",
		Handler: exit,
	},
	"clear": {
		Usage:   "clear screen",
		Handler: clear,
	},
	"help": {
		Usage: "list all the available commands",
		Handler: func(ctx *ExecContext, args []string) error {
			ctx.ListCommand()
			return nil
		},
	},
}

var routerCommands = map[string]Command{
	"disable": {
		Usage:   "disable peer -- disable a peer in network",
		Handler: disablePeer,
	},
	"enable": {
		Usage:   "enable peer - enable a peer in network",
		Handler: enablePeer,
	},
	"ls": {
		Usage:   "ls - list all peers in route table",
		Handler: listRouteTab,
	},

	"clean": {
		Usage:   "clean peer -- clean a peer's name in route table",
		Handler: cleanPeer,
	},

	"block": {
		Usage:   "block [enable/disable] - enable/disable block registration",
		Handler: blockToggle,
	},
}

var raftCommands = map[string]Command{
	"start": {
		Usage: "start the raft",
		Handler: func(ctx *ExecContext, args []string) error {
			return ctx.rf.Start()
		},
	},
	"timeout": {
		Usage:   "timeout [n sec/now] - show the timer status or timeout in n seconds or now",
		Handler: timeout,
	},
	"status": {
		Usage:   "show raft status",
		Handler: showStatus,
	},
	"log": {
		Usage:   "log start_idx [end_idx] - print of log[start_idx:end_idx], end_idx will be start_idx+1 by default",
		Handler: showLogs,
	},
	"commit": {
		Usage:   "commit [string] - commit a string log to a leader peer",
		Handler: addLog,
	},
}

func exit(ctx *ExecContext, _ []string) error {
	fmt.Println("\nexit...")
	ctx.Close()
	os.Exit(0)
	return nil
}

func clear(_ *ExecContext, _ []string) error {
	fmt.Fprintf(os.Stdout, "\u001b[2J")
	fmt.Fprintf(os.Stdout, "\u001b[1000A")
	return nil
}

func enablePeer(ctx *ExecContext, args []string) error {
	if len(args) < 1 {
		return errors.New("expect a peer name")
	}

	name := args[0]
	tab := ctx.router.GetRouterTable()
	_, in := tab[name]
	if !in {
		return errors.Errorf(`peer '%s' not found`, name)
	}

	ctx.router.Connect(name)
	return nil
}

func disablePeer(ctx *ExecContext, args []string) error {
	if len(args) < 1 {
		return errors.New("expect a peer name")
	}

	name := args[0]
	tab := ctx.router.GetRouterTable()
	_, in := tab[name]
	if !in {
		return errors.Errorf(`peer '%s' not found`, name)
	}

	ctx.router.Disconnect(name)
	return nil
}

func cleanPeer(ctx *ExecContext, args []string) error {
	if len(args) < 1 {
		return errors.New("expect a peer name")
	}

	name := args[0]
	ctx.router.Clean(name)
	return nil
}

func listRouteTab(ctx *ExecContext, args []string) error {
	tab := ctx.router.GetRouterTable()

	fmt.Printf("%-16s\t%-18s\t%-8s\t%-8s\n", "name", "address", "disabled", "latency")
	for name, info := range tab {
		fmt.Printf("%-16s\t%-18s\t%-8v\t%-8d\n",
			name, info.Addr, info.Disconnect, info.LatencyMs)
	}

	return nil
}

func blockToggle(ctx *ExecContext, args []string) error {
	if len(args) == 0 {
		fmt.Printf("%v\n", ctx.router.IsBlocked())
		return nil
	}

	switch args[0] {
	case "enable":
		ctx.router.SetBlock(true)
		fmt.Println("true")
	case "disable":
		ctx.router.SetBlock(false)
		fmt.Println("false")
	default:
		return errors.Errorf(`expect enable/disable, but got %v`, args[0])
	}
	return nil
}

func timeout(ectx *ExecContext, args []string) error {
	if !ectx.rf.Started() {
		return errors.New("raft not started")
	}
	if len(args) == 0 {
		showTimeout(ectx.rf)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := ectx.rf.GetStatus(ctx)
	if err != nil {
		return errors.WithMessage(err, "fail to get status")
	}

	if status.State != raft.FOLLOWER {
		return errors.Errorf("cannot reset timer of a %s", string(status.State))
	}
	if strings.ToUpper(strings.TrimSpace(args[0])) == "NOW" {
		ectx.rf.ResetTimer(ctx, -1)
		return nil
	}
	sec, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || sec <= 0 {
		return errors.Errorf("expect a positive number, but got %s", args[0])
	}
	ectx.rf.ResetTimer(ctx, time.Duration(sec)*time.Second)

	return nil
}

func addLog(ectx *ExecContext, args []string) error {
	if len(args) < 1 {
		return errors.New("expecting a string")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	idx, term, err := ectx.rf.CommitCommond(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("index=%d, term=%d\n", idx, term)
	return nil
}

func showLogs(ectx *ExecContext, args []string) error {
	if len(args) < 1 {
		return errors.New("need an start index at least")
	}
	if !ectx.rf.Started() {
		return errors.New("raft not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logs, err := ectx.rf.GetLogEntries(ctx)
	if err != nil {
		return errors.WithMessage(err, "fail to get logs")
	}

	var start, end int

	start, err = strconv.Atoi(args[0])
	if err != nil {
		return errors.Errorf("expecting a positive number, but got %s", args[0])
	}
	if len(logs) == 0 {
		fmt.Println("no logs yet")
		return nil
	}
	if start < 0 || start >= len(logs) {
		return errors.Errorf("out of range, expecting an index in [0, %d)", len(logs))
	}

	if len(args) > 1 {
		end, err = strconv.Atoi(args[1])
		if err != nil {
			return errors.Errorf("expecting a positive number, but got %s", args[1])
		}
		if end > len(logs) {
			end = len(logs)
		}
	} else {
		end = start + 1
	}

	fmt.Printf("%-5s\t%-4s\t%-s\n", "index", "term", "command")
	for i := start; i < end; i++ {
		l := logs[i]
		fmt.Printf("%-5d\t%-4d\t%-v\n", l.Index, l.Term, l.Command)
	}
	return nil
}

func showStatus(ectx *ExecContext, args []string) error {
	if !ectx.rf.Started() {
		return errors.New("raft not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	status, err := ectx.rf.GetStatus(ctx)
	if err != nil {
		return errors.WithMessage(err, "fail to get status")
	}
	infos := make([]string, 0, 4)
	infos = append(infos, fmt.Sprintf("%-12s : %-s", "name", status.Name))
	infos = append(infos, fmt.Sprintf("%-12s : %-s", "state", status.State.String()))
	infos = append(infos, fmt.Sprintf("%-12s : %-s", "vote_for", status.VoteFor))
	infos = append(infos, fmt.Sprintf("%-12s : %-d", "term", status.Term))
	infos = append(infos, fmt.Sprintf("%-12s : %-.2fsec", "timeout", float64(status.Timeout.Milliseconds())/float64(1000)))
	infos = append(infos, fmt.Sprintf("%-12s : %-s", "last_rst", status.LastReset.Format("15:04:05.000")))
	fmt.Println(strings.Join(infos, "\n"))
	return nil
}

func showTimeout(rf *raft.Raft) error {
	closeCh := make(chan struct{})

	go func() {
		reader := bufio.NewReader(os.Stdin)
		reader.ReadLine()
		close(closeCh)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := rf.GetStatus(ctx)
	if err != nil {
		return err
	}
	lastReset := status.LastReset
	timeout := status.Timeout

loop:
	for time.Since(lastReset) <= timeout {
		passed := time.Since(lastReset)
		progressBar(fmt.Sprintf("%.2fs", float64(timeout.Milliseconds())/1000),
			int(passed.Milliseconds()), int(timeout.Milliseconds()))

		select {
		case <-closeCh:
			return nil
		case <-time.After(100 * time.Millisecond):
			c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			status, err := rf.GetStatus(c)
			if err != nil {
				return err
			}
			if status.LastReset != lastReset {
				progressBar(fmt.Sprintf("%.2fs", float64(timeout.Milliseconds())/1000),
					int(passed.Milliseconds()), int(timeout.Milliseconds()))
				timeout = status.Timeout
				lastReset = status.LastReset
				println()
				goto loop
			}
			continue
		}
	}
	println()
	return nil
}

func progressBar(prefix string, progress, total int) {
	per := (progress * 100) / total

	cols := int(float64(cols()) / 2)
	fills := per * cols / 100
	buf := make([]byte, cols)
	for i := 0; i < cols; i++ {
		if i < fills {
			buf[i] = '#'
		} else {
			buf[i] = ' '
		}
	}

	fmt.Printf("\r%s [%s] %d%%", prefix, string(buf), per)
}

func cols() int64 {
	cmd := exec.Command("tput", "cols")
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	col, _ := strconv.ParseInt(string(strings.TrimSpace(string(out))), 10, 64)
	return col
}
