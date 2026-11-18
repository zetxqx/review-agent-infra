package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/zetxqx/review-agent-infra/internal/loader"
)

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: loader <wait|unpack-src|prepare|read-report> [flags]")
	}
	defaults := loader.DefaultPaths()
	switch args[0] {
	case "wait":
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		<-sigCh
		return nil
	case "unpack-src":
		fs := flag.NewFlagSet("unpack-src", flag.ContinueOnError)
		srcDir := fs.String("src-dir", defaults.SrcDir, "")
		stripTop := fs.Bool("strip-top", true, "")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return loader.UnpackSourceTGZ(stdin, *srcDir, *stripTop)
	case "prepare":
		fs := flag.NewFlagSet("prepare", flag.ContinueOnError)
		srcDir := fs.String("src-dir", defaults.SrcDir, "")
		inDir := fs.String("in-dir", defaults.InDir, "")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return loader.Prepare(stdin, loader.Paths{SrcDir: *srcDir, InDir: *inDir, OutDir: defaults.OutDir})
	case "read-report":
		fs := flag.NewFlagSet("read-report", flag.ContinueOnError)
		outDir := fs.String("out-dir", defaults.OutDir, "")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return loader.ReadReport(stdout, *outDir)
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
