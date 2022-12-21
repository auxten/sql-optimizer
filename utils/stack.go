package utils

type Stack []interface{}

// NewStack returns a new Stack.
func NewStack(capacity int) *Stack {
	if capacity < 0 {
		capacity = 0
	}
	s := make(Stack, 0, capacity)
	return &s
}

func (s *Stack) Copy(start, end int) *Stack {
	stack := make(Stack, end-start)
	copy(stack, (*s)[start:end])
	return &stack
}

// Len returns the length of Stack.
func (s *Stack) Len() int {
	return len(*s)
}

// Push a new value onto the Stack.
func (s *Stack) Push(node interface{}) {
	*s = append(*s, node)
}

// Pop removes and return top element of Stack. Return nil, false if Stack is empty.
func (s *Stack) Pop() (interface{}, bool) {
	if s.Len() == 0 {
		return nil, false
	} else {
		index := s.Len() - 1
		element := (*s)[index]
		*s = (*s)[:index]
		return element, true
	}
}

// Get read the value at idx from Stack without modifying the Stack. Return nil, false if idx out of range.
func (s *Stack) Get(idx int) (interface{}, bool) {
	if idx >= s.Len() {
		return nil, false
	} else {
		return (*s)[idx], true
	}
}
