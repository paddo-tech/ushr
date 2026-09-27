// Command ushr is the user-facing CLI. The daemons live in ushr-controller
// and ushr-agent; this binary holds setup, status, and other interactive flows.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/paddo-tech/ushr/internal/version"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "setup":
		if err := runSetup(ctx, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "init":
		if err := runInit(ctx, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "login":
		if err := runLogin(ctx, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "doctor":
		if err := runDoctor(ctx, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "cost":
		if err := runCost(ctx, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "version", "--version", "-version":
		fmt.Println("ushr", version.Version)
	case "help", "--help", "-h":
		usage()
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: ushr <subcommand>

subcommands:
  login    set up this host end to end for the hosted plane: config,
           prereqs, browser enrollment, GitHub App, service start
  setup    create a GitHub App for an org/repo and record it in agent.yaml
           (local/OSS path; login runs this automatically when needed)
  doctor   check config, prereqs, service, and control-plane reachability
  init     rewrite a repo's workflow runs-on targets to ushr labels
  cost     report runner-minutes served and $ saved vs GitHub-hosted
  version  print version
  help     print this message`)
}
