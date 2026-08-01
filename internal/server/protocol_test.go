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
		{
			name:  "get",
			input: "GET foo",
			want: Command{
				Type: CmdGet,
				Key:  "foo",
			},
		},
		{
			name:  "getWithSpace",
			input: "GET  foo",
			want: Command{
				Type: CmdGet,
				Key:  "foo",
			},
		},
		{
			name:  "set",
			input: "SET foo bar",
			want: Command{
				Type:  CmdSet,
				Key:   "foo",
				Value: "bar",
			},
		},
		{
			name:  "setInternalSpaces",
			input: "SET foo bar  baz",
			want: Command{
				Type:  CmdSet,
				Key:   "foo",
				Value: "bar  baz",
			},
		},
		{
			name:  "setMultipleValues",
			input: "SET foo bar baz",
			want: Command{
				Type:  CmdSet,
				Key:   "foo",
				Value: "bar baz",
			},
		},
		{
			name:  "delete",
			input: "delete foo",
			want: Command{
				Type: CmdDelete,
				Key:  "foo",
			},
		},
		{
			name:    "deleteExtraArg",
			input:   "DELETE foo bar",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: ErrEmptyCommand,
		},
		{
			name:    "unknown",
			input:   "PING foo",
			wantErr: ErrUnknownCommand,
		},
		{
			name:    "wrongNumArgsTooLittle",
			input:   "GET",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:    "wrongNumArgsTooBig",
			input:   "GET foo bar",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:  "replicaGet",
			input: "REPLICA_GET foo",
			want: Command{
				Type: CmdReplicaGet,
				Key:  "foo",
			},
		},
		{
			name:    "replicaGetMissingKey",
			input:   "REPLICA_GET",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:    "replicaGetExtraArg",
			input:   "REPLICA_GET foo bar",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:  "replicaSet",
			input: "REPLICA_SET foo 500 node-a bar",
			want: Command{
				Type:      CmdReplicaSet,
				Key:       "foo",
				Value:     "bar",
				Timestamp: 500,
				NodeID:    "node-a",
			},
		},
		{
			name:  "replicaSetMultipleValues",
			input: "REPLICA_SET foo 500 node-a bar baz",
			want: Command{
				Type:      CmdReplicaSet,
				Key:       "foo",
				Value:     "bar baz",
				Timestamp: 500,
				NodeID:    "node-a",
			},
		},
		{
			name:    "replicaSetMissingMetadata",
			input:   "REPLICA_SET foo bar",
			wantErr: ErrWrongNumArgs,
		},
		{
			name:  "replicaDelete",
			input: "REPLICA_DELETE foo 600 node-a",
			want: Command{
				Type:      CmdReplicaDelete,
				Key:       "foo",
				Timestamp: 600,
				NodeID:    "node-a",
			},
		},
		{
			name:    "replicaDeleteMissingMetadata",
			input:   "REPLICA_DELETE foo",
			wantErr: ErrWrongNumArgs,
		},
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
