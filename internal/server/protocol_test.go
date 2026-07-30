package server

import (
	"errors"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Command
		wantErr error
	}{
		{"get", "GET foo", Command{Type: CmdGet, Key: "foo"}, nil},
		{"getWithSpace", "GET  foo", Command{Type: CmdGet, Key: "foo"}, nil},
		{"set", "SET foo bar", Command{Type: CmdSet, Key: "foo", Value: "bar"}, nil},
		{"setInternalSpaces", "SET foo bar  baz", Command{Type: CmdSet, Key: "foo", Value: "bar  baz"}, nil},
		{"setMultipleValues", "SET foo bar baz", Command{Type: CmdSet, Key: "foo", Value: "bar baz"}, nil},
		// also testing lowercase commands
		{"delete", "delete foo", Command{Type: CmdDelete, Key: "foo"}, nil},
		{"deleteExtraArg", "DELETE foo bar", Command{}, ErrWrongNumArgs},
		{"empty", "", Command{}, ErrEmptyCommand},
		{"unknown", "PING foo", Command{}, ErrUnknownCommand},
		{"wrongNumArgsTooLittle", "GET", Command{}, ErrWrongNumArgs},
		{"wrongNumArgsTooBig", "GET foo bar", Command{}, ErrWrongNumArgs},
		{"replicaGet", "REPLICA_GET foo", Command{Type: CmdReplicaGet, Key: "foo"}, nil},
		{"replicaGetMissingKey", "REPLICA_GET", Command{}, ErrWrongNumArgs},
		{"replicaGetExtraArg", "REPLICA_GET foo bar", Command{}, ErrWrongNumArgs},
		{"replicaSet", "REPLICA_SET foo bar", Command{Type: CmdReplicaSet, Key: "foo", Value: "bar"}, nil},
		{"replicaSetMultipleValues", "REPLICA_SET foo bar baz", Command{Type: CmdReplicaSet, Key: "foo", Value: "bar baz"}, nil},
		{"replicaSetMissingValue", "REPLICA_SET foo", Command{}, ErrWrongNumArgs},
		{"replicaDelete", "REPLICA_DELETE foo", Command{Type: CmdReplicaDelete, Key: "foo"}, nil},
		{"replicaDeleteMissingKey", "REPLICA_DELETE", Command{}, ErrWrongNumArgs},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse(test.input)

			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"Parse(%q) error = %v, want %v",
					test.input,
					err,
					test.wantErr,
				)
			}

			if got != test.want {
				t.Errorf(
					"Parse(%q) got %+v, want %+v",
					test.input,
					got,
					test.want,
				)
			}
		})
	}
}
