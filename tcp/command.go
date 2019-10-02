package tcp

import (
	"errors"
	"strings"

	"github.com/ryansann/hydro/index"
)

type commandType int

const (
	get = iota
	set
	del
	quit
)

var commands = map[commandType]string{
	get:  "get",
	set:  "set",
	del:  "del",
	quit: "quit",
}

type command struct {
	op  commandType
	key string
	val string
}

// parseCommand accepts the line as input and returns a command object or an error if it could not parse the line.
func parseCommand(line string) (*command, error) {
	cmps := strings.Split(line, " ")
	if len(cmps) < 1 {
		return nil, errors.New("command must have an operation")
	}

	cmd := &command{}
	switch strings.ToLower(cmps[0]) {
	case commands[get]:
		cmd.op = commandType(get)
		if len(cmps) != 2 {
			return nil, errors.New("get command requires an argument")
		}
		cmd.key = cmps[1]
	case commands[set]:
		cmd.op = commandType(set)
		if len(cmps) != 3 {
			return nil, errors.New("set command requires 2 arguments")
		}
		cmd.key = cmps[1]
		cmd.val = cmps[2]
	case commands[del]:
		cmd.op = commandType(del)
		if len(cmps) != 2 {
			return nil, errors.New("del command requires an argument")
		}
		cmd.key = cmps[1]
	case commands[quit]:
		cmd.op = commandType(quit)
		if len(cmps) != 1 {
			return nil, errors.New("quit command should not have any arguments")
		}
	default:
		return nil, errors.New("unrecognized operation")
	}

	return cmd, nil
}

// execute runs the command by communicating directly with the Indexer.
func (cmd *command) execute(c chan struct{}, i index.Indexer) (string, error) {
	switch cmd.op {
	case get:
		res, err := i.Get(cmd.key)
		if err != nil {
			return "", err
		}
		return string(res), nil
	case set:
		err := i.Set(cmd.key, cmd.val)
		if err != nil {
			return "", err
		}
		return "", nil
	case del:
		err := i.Del(cmd.key)
		if err != nil {
			return "", err
		}
		return "", nil
	case quit:
		close(c)
		return "", errors.New("closing")
	default:
		return "", errors.New("did not execute")
	}
}
