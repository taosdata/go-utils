package trie

import (
	"bytes"
	"testing"
)

func TestTrieAdd(t *testing.T) {
	trie := NewDefault()
	n, err := trie.Add("foo", 1)
	if err != nil {
		t.Fatal("Add get error", err)
	}
	if *n.value != 1 {
		t.Errorf("Expected 1, got: %d", *n.value)
	}
}

func TestTrieAddFail(t *testing.T) {
	trie := NewDefault()
	n, err := trie.Add("foo", 1)
	if err != nil {
		t.Fatal("Add get error", err)
	}
	if *n.value != 1 {
		t.Errorf("Expected 1, got: %d", *n.value)
	}
	_, err = trie.Add("foo", 2)
	if err == nil {
		t.Fatal("Expected error")
	}
	n, err = trie.Add("foo", 1)
	if err != nil {
		t.Fatal("Expected no error")
	}
	if *n.value != 1 {
		t.Errorf("Expected 1, got: %d", *n.value)
	}
	_, err = trie.Add("foo.*.1", 1)
	if err == nil {
		t.Fatal("Expected error")
	}
	_, err = trie.Add("foo.2", -1)
	if err == nil {
		t.Fatal("Expected error")
	}
	b := bytes.NewBufferString("")
	for i := 0; i < MaxLength+1; i++ {
		b.WriteByte('a')
	}
	_, err = trie.Add(b.String(), 3)
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestTrieFind(t *testing.T) {
	trie := NewDefault()
	trie.Add("foo", 1)

	n, ok := trie.Find("foo")
	if ok != true {
		t.Fatal("Could not find node")
	}
	if n == nil {
		t.Fatal("")
	}
	if *n != 1 {
		t.Errorf("Expected 1, got: %d", *n)
	}
}

func TestTrieFindMissing(t *testing.T) {
	trie := NewDefault()
	n, ok := trie.Find("foo")
	if ok != false {
		t.Errorf("Expected ok to be false")
	}
	if n != nil {
		t.Errorf("Expected nil, got: %v", n)
	}
}

func TestTrieFindWildcard(t *testing.T) {
	trie := NewDefault()
	_, _ = trie.Add("A.?.C.*", 2)
	n, ok := trie.Find("A.B.C")
	if ok != true {
		t.Fatal("Could not find node")
	}
	if n == nil {
		t.Fatal("Expected not nil")
	}
	if *n != 2 {
		t.Errorf("Expected 2, got: %d", *n)
	}
}

func TestTrie_Find(t1 *testing.T) {
	trie := NewDefault()
	_, _ = trie.Add("A.?.C.?.*", 1)
	_, _ = trie.Add("A.B.C.?.*", 2)
	_, _ = trie.Add("A.B.C.?.?.*", 3)
	value1 := 1
	value2 := 2
	value3 := 3
	data := map[string][2]interface{}{
		"":            {nil, false},
		"A.B.C":       {nil, true},
		"A.B.C.D":     {&value2, true},
		"A.B.C.D.E":   {&value3, true},
		"A.B.C.D.E.F": {&value3, true},
		"A.B.D":       {nil, false},
		"A.B.B.C":     {nil, false},
		"A.A.C.D":     {&value1, true},
		"A.A.C.D.E":   {&value1, true},
		"A.A.C.D.E.F": {&value1, true},

		"A.B.C.*":     {&value2, true},
		"A.B.C.D.*":   {&value3, true},
		"A.B.C.*.D":   {&value3, true},
		"A.B.C.*.*.*": {&value3, true},
		"A.B.C.A.B.C": {&value3, true},
		"A.B.C.?":     {&value2, true},
	}

	t1.Run("test", func(t1 *testing.T) {
		for k, v := range data {
			value, exist := trie.Find(k)
			if v[1].(bool) != exist {
				t1.Errorf("%s Expect %t", k, v[1].(bool))
				return
			}
			var vv *int
			if v[0] != nil {
				vv = v[0].(*int)
			}
			if vv == nil {
				if value != nil {
					t1.Errorf("%s Expect nil", k)
					return
				}
			} else {
				if value == nil {
					t1.Errorf("%s Expectd %d,got nil", k, *vv)
					return

				} else if *value != *vv {
					t1.Errorf("%s Expectd %d,got %d", k, *vv, *value)
					return
				}
			}
		}
	})
}

func Test_findNode(t *testing.T) {
	trie := NewDefault()
	if got := trie.findNode(nil, []string{""}, 0); got != nil {
		t.Errorf("findNode() = %v, want %v", got, nil)
	}
}

func TestNode_Children(t *testing.T) {
	trie := NewDefault()
	d := map[string]int{
		"A": 1,
		"B": 1,
		"C": 1,
	}
	for s, i := range d {
		_, _ = trie.Add(s, i)
	}
	children := trie.Root().Children()
	if len(children) != len(d) {
		t.Errorf("unexpect children length %d wanted %d", len(children), len(d))
		return
	}
	for _, child := range children {
		_, exist := d[child]
		if !exist {
			t.Fatal("unexpect child ", child)
		}
	}
}

func TestTrie_FindCustomize(t1 *testing.T) {
	trie, err := New("_", "%", "-", MaxLength)
	if err != nil {
		t1.Error(err)
		return
	}
	_, _ = trie.Add("A-_-C-_-%", 1)
	_, _ = trie.Add("A-B-C-_-%", 2)
	_, _ = trie.Add("A-B-C-_-_-%", 3)
	value1 := 1
	value2 := 2
	value3 := 3
	data := map[string][2]interface{}{
		"":            {nil, false},
		"A-B-C":       {nil, true},
		"A-B-C-D":     {&value2, true},
		"A-B-C-D-E":   {&value3, true},
		"A-B-C-D-E-F": {&value3, true},
		"A-B-D":       {nil, false},
		"A-B-B-C":     {nil, false},
		"A-A-C-D":     {&value1, true},
		"A-A-C-D-E":   {&value1, true},
		"A-A-C-D-E-F": {&value1, true},

		"A-B-C-%":     {&value2, true},
		"A-B-C-D-%":   {&value3, true},
		"A-B-C-%-D":   {&value3, true},
		"A-B-C-%-%-%": {&value3, true},
		"A-B-C-A-B-C": {&value3, true},
		"A-B-C-_":     {&value2, true},
	}

	t1.Run("test", func(t1 *testing.T) {
		for k, v := range data {
			value, exist := trie.Find(k)
			if v[1].(bool) != exist {
				t1.Errorf("%s Expect %t", k, v[1].(bool))
				return
			}
			var vv *int
			if v[0] != nil {
				vv = v[0].(*int)
			}
			if vv == nil {
				if value != nil {
					t1.Errorf("%s Expect nil", k)
					return
				}
			} else {
				if value == nil {
					t1.Errorf("%s Expectd %d,got nil", k, *vv)
					return

				} else if *value != *vv {
					t1.Errorf("%s Expectd %d,got %d", k, *vv, *value)
					return
				}
			}
		}
	})
}

func TestTrie_NewError(t *testing.T) {
	_, err := New("", "", "", -1)
	if err == nil {
		t.Error("expect error")
		return
	}
	_, err = New(SingleTokenWildcard, "", "", -1)
	if err == nil {
		t.Error("expect error")
		return
	}
	_, err = New(SingleTokenWildcard, MultipleTokenWildcard, "", -1)
	if err == nil {
		t.Error("expect error")
		return
	}
	_, err = New(SingleTokenWildcard, MultipleTokenWildcard, Sep, -1)
	if err == nil {
		t.Error("expect error")
		return
	}
	_, err = New(SingleTokenWildcard, MultipleTokenWildcard, Sep, MaxLength)
	if err != nil {
		t.Error("unexpect error", err)
		return
	}
}
