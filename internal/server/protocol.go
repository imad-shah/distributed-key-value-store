package server

import (
	"errors"
	"fmt"
	"strings"
)

type CommandType string

type Command struct {
	Type  CommandType
	Key   string
	Value string
}

var (
	ErrEmptyCommand   = errors.New("empty command")
	ErrUnknownCommand = errors.New("unknown command")
	ErrWrongNumArgs   = errors.New("wrong number of arguments")
)

const (
	CmdGet           CommandType = "GET"
	CmdReplicaGet    CommandType = "REPLICA_GET"
	CmdSet           CommandType = "SET"
	CmdReplicaSet    CommandType = "REPLICA_SET"
	CmdDelete        CommandType = "DELETE"
	CmdReplicaDelete CommandType = "REPLICA_DELETE"
	CmdUnknown       CommandType = "UNKNOWN"
)

func parseCommandType(s string) CommandType {
	switch strings.ToUpper(s) {
	case "GET":
		return CmdGet
	case "REPLICA_GET":
		return CmdReplicaGet
	case "SET":
		return CmdSet
	case "REPLICA_SET":
		return CmdReplicaSet
	case "DELETE":
		return CmdDelete
	case "REPLICA_DELETE":
		return CmdReplicaDelete
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
	case CmdGet, CmdReplicaGet, CmdDelete, CmdReplicaDelete:
		key, extra := splitFirst(rest)
		if key == "" || extra != "" {
			return Command{}, fmt.Errorf("%w for %s", ErrWrongNumArgs, cmdWord)
		}
		return Command{Type: cmdType, Key: key}, nil

	case CmdSet, CmdReplicaSet:
		key, value := splitFirst(rest)
		if key == "" || value == "" {
			return Command{}, fmt.Errorf("%w for %s", ErrWrongNumArgs, cmdWord)
		}
		return Command{Type: cmdType, Key: key, Value: value}, nil

	default:
		return Command{}, fmt.Errorf("%w: %q", ErrUnknownCommand, cmdWord)
	}
}
