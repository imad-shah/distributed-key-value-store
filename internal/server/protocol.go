package server

import (
	"fmt"
	"strings"
	"errors"
)

type CommandType string

type Command struct {
	Type CommandType
	Key string
	Value string
}

var (
	ErrEmptyCommand = errors.New("Empty Command")
	ErrUnknownCommand = errors.New("Unknown Command")
	ErrWrongNumArgs = errors.New("Wrong number of arguments")
)

const (
	CmdGet CommandType = "GET"
	CmdSet CommandType = "SET"
	CmdDelete CommandType = "DELETE"
	CmdUnknown CommandType = "UNKNOWN"
)

func parseCommandType(s string) CommandType {
	switch strings.ToUpper(s) {
	case "GET":
		return CmdGet
	case "SET":
		return CmdSet
	case "DELETE":
		return CmdDelete
	default:
		return CmdUnknown
	}
}

func splitFirst(s string) (token, rest string) {
	s = strings.TrimLeft(s, " \t")
	i := strings.IndexAny(s, " \t")
	if i < 0 {
		return s, ""
	}
	return s[:i], strings.TrimLeft(s[i:], " \t")
}

func Parse(raw string) (Command, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Command{}, ErrEmptyCommand
	}

	cmdWord, rest := splitFirst(trimmed)
	cmdType := parseCommandType(cmdWord)

	switch cmdType {
	case CmdGet, CmdDelete:
		key, extra := splitFirst(rest)
		if key == "" || extra != "" {
			return Command{}, fmt.Errorf("%w for %q", ErrWrongNumArgs, cmdWord)
		}
		return Command{Type: cmdType, Key: key}, nil

	case CmdSet:
		key, value := splitFirst(rest)
		if key == "" || value == "" {
			return Command{}, fmt.Errorf("%w for 'SET'", ErrWrongNumArgs)
		}
		return Command{Type: CmdSet, Key: key, Value: value}, nil

	default:
		return Command{}, fmt.Errorf("%w: %q", ErrUnknownCommand, cmdWord)
	}
}
