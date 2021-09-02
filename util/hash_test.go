package util

import "testing"

func TestToHashString(t *testing.T) {
	type args struct {
		source string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "success",
			args: args{source: "test"},
			want: "md5_098f6bcd4621d373cade4e832627b4f6",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToHashString(tt.args.source); got != tt.want {
				t.Errorf("ToHashString() = %v, want %v", got, tt.want)
			}
		})
	}
}
