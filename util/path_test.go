package util

import "testing"

func TestPathExist(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "success",
			args: args{name: "./path.go"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathExist(tt.args.name); got != tt.want {
				t.Errorf("PathExist() = %v, want %v", got, tt.want)
			}
		})
	}
}
