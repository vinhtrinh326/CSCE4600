package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strings"
)

func main() {
	exit := make(chan struct{}, 2) // buffer this so there's no deadlock.
	runLoop(os.Stdin, os.Stdout, os.Stderr, exit)
}

func runLoop(r io.Reader, w, errW io.Writer, exit chan struct{}) {
	var (
		input    string
		err      error
		readLoop = bufio.NewReader(r)
	)
	for {
		select {
		case <-exit:
			_, _ = fmt.Fprintln(w, "exiting gracefully...")
			return
		default:
			if err := printPrompt(w); err != nil {
				_, _ = fmt.Fprintln(errW, err)
				continue
			}
			if input, err = readLoop.ReadString('\n'); err != nil {
				_, _ = fmt.Fprintln(errW, err)
				continue
			}
			if err = handleInput(w, input, exit); err != nil {
				_, _ = fmt.Fprintln(errW, err)
			}
		}
	}
}

func printPrompt(w io.Writer) error {
	u, err := user.Current()
	if err != nil {
		return err
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%v [%v] $ ", wd, u.Username)
	return err
}

func handleInput(w io.Writer, input string, exit chan<- struct{}) error {
	input = strings.TrimSpace(input)
	args := strings.Split(input, " ")
	name, args := args[0], args[1:]

	switch name {
	case "cd":
		return changeDirectory(args...)
	case "env":
		return environmentVariables(w, args...)
	case "exit":
		exit <- struct{}{}
		return nil
	case "echo":
		return echo(w, args...)
	case "pwd":
		return printWorkingDirectory(w)
	case "export":
		return exportVariable(w, args...)
	case "unset":
		return unsetVariable(args...)
	case "history":
		return showHistory(w)
	}

	return executeCommand(name, args...)
}

func executeCommand(name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

// Implementations of the built-in commands:

func changeDirectory(args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no path provided")
	}
	return os.Chdir(args[0])
}

func environmentVariables(w io.Writer, args ...string) error {
	for _, env := range os.Environ() {
		_, err := fmt.Fprintln(w, env)
		if err != nil {
			return err
		}
	}
	return nil
}

func echo(w io.Writer, args ...string) error {
	_, err := fmt.Fprintln(w, strings.Join(args, " "))
	return err
}

func printWorkingDirectory(w io.Writer) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, wd)
	return err
}

func exportVariable(w io.Writer, args ...string) error {
	// This is a placeholder; setting environment variables in Go is not straightforward.
	return nil
}

func unsetVariable(args ...string) error {
	// This is a placeholder; unsetting environment variables in Go is not straightforward.
	return nil
}

func showHistory(w io.Writer) error {
	// This is a placeholder; implementing history requires additional logic.
	return nil
}
