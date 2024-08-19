package denodo_test

import (
	"quollio-reverse-agent/connector/denodo"
	"testing"

	testifyAssert "github.com/stretchr/testify/assert"
)

func TestIsSkipUpdateDatabaseByFilter(t *testing.T) {
	type args struct {
		targetDBList []string
		targetDB     string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "targetDBList is empty",
			args: args{
				targetDBList: []string{},
				targetDB:     "testDB",
			},
			want: false,
		},
		{
			name: "targetDBList is not empty and the db is contained in the array",
			args: args{
				targetDBList: []string{"testDB"},
				targetDB:     "testDB",
			},
			want: false,
		},
		{
			name: "targetDBList is not empty and the db is not contained in the array",
			args: args{
				targetDBList: []string{"testDB1"},
				targetDB:     "testDB",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := denodo.DenodoConnector{
				DenodoQueryTargetDBs: tt.args.targetDBList,
			}
			res := c.IsSkipUpdateDatabaseByFilter(tt.args.targetDB)
			testifyAssert.Equal(t, res, tt.want)
		})
	}
}
