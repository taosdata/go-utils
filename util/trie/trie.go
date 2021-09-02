package trie

//Not thread safe !!!

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// SingleTokenWildcard 指代一个
	SingleTokenWildcard = "?"
	// MultipleTokenWildcard 指代零个或任意多个
	MultipleTokenWildcard = "*"
	Sep                   = "."
	MaxLength             = 191
)

type Node struct {
	name     string
	parent   *Node
	children map[string]*Node
	value    *int
}

type Trie struct {
	root                  *Node
	singleTokenWildcard   string
	multipleTokenWildcard string
	sep                   string
	maxLength             int
}

func NewDefault() *Trie {
	return &Trie{
		root: &Node{
			children: make(map[string]*Node),
		},
		singleTokenWildcard:   SingleTokenWildcard,
		multipleTokenWildcard: MultipleTokenWildcard,
		sep:                   Sep,
		maxLength:             MaxLength,
	}
}

func New(singleTokenWildcard string, multipleTokenWildcard string, sep string, maxLength int) (*Trie, error) {
	if singleTokenWildcard == "" {
		return nil, errors.New("singleTokenWildcard required")
	}
	if multipleTokenWildcard == "" {
		return nil, errors.New("multipleTokenWildcard required")
	}
	if sep == "" {
		return nil, errors.New("sep required")
	}
	if maxLength <= 0 {
		return nil, errors.New("maxLength should positive")
	}
	return &Trie{
		root: &Node{
			children: make(map[string]*Node),
		},
		singleTokenWildcard:   singleTokenWildcard,
		multipleTokenWildcard: multipleTokenWildcard,
		sep:                   sep,
		maxLength:             maxLength,
	}, nil
}

func (t *Trie) Root() *Node {
	return t.root
}

func (t *Trie) Add(key string, value int) (*Node, error) {
	if len(key) > t.maxLength {
		return nil, fmt.Errorf("the length of the key cannot exceed %d", t.maxLength)
	}
	if value < 0 {
		return nil, fmt.Errorf("value must be a positive integer")
	}
	paths := strings.Split(key, t.sep)
	node := t.root
	for i := range paths {
		r := paths[i]
		//"*"仅允许出现在最后一个字符
		if r == t.multipleTokenWildcard && i != len(paths)-1 {
			return nil, fmt.Errorf("multiple token wildcard %s must only appear at the end", t.multipleTokenWildcard)
		}
		if n, ok := node.children[r]; ok {
			if i == len(paths)-1 {
				//最后一个元素检查是否value匹配上
				if n.value != nil && *n.value != value {
					return nil, fmt.Errorf("%s have dirrerent value %d and %d ", key, *n.value, value)
				}
			}
			node = n
		} else {
			if i == len(paths)-1 {
				node = node.NewChild(r, &value)
			} else {
				node = node.NewChild(r, nil)
			}
		}
	}
	return node, nil
}

// Find 存在找到但非叶子节点
func (t *Trie) Find(key string) (*int, bool) {
	if len(key) == 0 {
		return nil, false
	}
	node := t.findNode(t.Root(), strings.Split(key, t.sep), 0)
	if node == nil {
		return nil, false
	}
	if node.value == nil {
		//定义如 A.*会产生节点 root->A->*,此时A节点value为nil, 搜索 "A"时需要向下查找看看是不是有*节点
		child, exist := node.children[t.multipleTokenWildcard]
		if exist {
			return child.value, true
		}
	}
	return node.value, true
}

func (t *Trie) findNode(node *Node, path []string, index int) *Node {
	if node == nil {
		return nil
	}
	var n *Node
	var ok bool
	if path[index] == t.multipleTokenWildcard {
		// "*"不做名称匹配优先单匹配"?"
		n, ok = node.children[t.singleTokenWildcard]
	} else {
		n, ok = node.children[path[index]]
	}
	if !ok {
		//检查单匹配
		n, ok = node.children[t.singleTokenWildcard]
		if !ok {
			//检查全匹配
			n, ok = node.children[t.multipleTokenWildcard]
			if !ok {
				return nil
			} else {
				return n
			}
		}
	}
	if index == len(path)-1 {
		return n
	}
	index += 1
	return t.findNode(n, path, index)
}

func (n *Node) NewChild(val string, value *int) *Node {
	node := &Node{
		name:     val,
		parent:   n,
		children: make(map[string]*Node),
	}
	if value != nil {
		node.value = value
	}
	n.children[node.name] = node
	return node
}

func (n *Node) Children() []string {
	var r []string
	for s := range n.children {
		r = append(r, s)
	}
	return r
}
