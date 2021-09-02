package util

import "testing"

func TestEscapeString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "success",
			args: args{s: "'b'"},
			want: `\'b\'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EscapeString(tt.args.s); got != tt.want {
				t.Errorf("EscapeString() = %v, want %v", got, tt.want)
			}
		})
	}
}
