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
		{"get", "GET foo", Command{Type: "GET", Key: "foo"}, nil},
		{"getWithSpace", "GET  foo", Command{Type: "GET", Key: "foo"}, nil},
		{"set", "SET foo bar", Command{Type: "SET", Key: "foo", Value: "bar"}, nil},
		{"setInternalSpaces", "SET foo bar  baz", Command{Type: "SET", Key: "foo", Value: "bar  baz"}, nil},
		// also testing lowercase commands
		{"delete", "delete foo", Command{Type: "DELETE", Key: "foo"}, nil},
		{"deleteExtraArg", "DELETE foo bar", Command{}, ErrWrongNumArgs},
		{"setMultipleValues", "SET foo bar baz", Command{Type: "SET", Key: "foo", Value: "bar baz"}, nil},
		{"empty", "", Command{}, ErrEmptyCommand},
		{"unknown", "PING foo", Command{}, ErrUnknownCommand},
		{"wrongNumArgsTooLittle", "GET", Command{}, ErrWrongNumArgs},
		{"wrongNumArgsTooBig", "GET foo bar", Command{}, ErrWrongNumArgs},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Parse(test.input)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Parse(%q) error = %v, want %v", test.input, err, test.wantErr)
			}
			if got != test.want {
				t.Errorf("Parse(%q) got %+v, want %+v", test.input, got, test.want)
			}
		})
	}
}
