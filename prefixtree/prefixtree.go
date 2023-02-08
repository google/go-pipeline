// Package prefixtree defines a simple prefix tree used in the pipeline demo.
package prefixtree

// Node is a prefix tree node.
type Node struct {
	Count    int
	IsWord   bool
	Children map[byte]*Node
}

// New creates and returns a new, empty Node.
func New() *Node {
	return &Node{
		Children: map[byte]*Node{},
	}
}

// Clear clears out the receiver, recursively emptying (but not deleting)
// child nodes.
func (ptn *Node) Clear() {
	if ptn.Count > 0 {
		for _, child := range ptn.Children {
			child.Clear()
		}
	}
	ptn.Count = 0
	ptn.IsWord = false
}

// MergeFrom updates the receiver recursively from the argument.
func (ptn *Node) MergeFrom(other *Node) {
	ptn.Count += other.Count
	ptn.IsWord = ptn.IsWord || other.IsWord
	for char, otherChild := range other.Children {
		if otherChild.Count == 0 {
			continue
		}
		child, ok := ptn.Children[char]
		if !ok {
			child = New()
			ptn.Children[char] = child
		}
		child.MergeFrom(otherChild)
	}
}

// Insert inserts the specified string into this node.  If the receiver is
// the root of a tree, 'suffix' is a whole word; otherwise, it's a suffix
// after discarding as many prefix characters as the receiver node is deep.
func (n *Node) Insert(suffix string) {
	n.Count++
	if len(suffix) == 0 {
		n.IsWord = true
		return
	}
	char := suffix[0]
	suffix = suffix[1:]
	child, ok := n.Children[char]
	if !ok {
		child = New()
		n.Children[char] = child
	}
	child.Insert(suffix)
}
