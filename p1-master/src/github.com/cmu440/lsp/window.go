package lsp

import ()

// utility class of the buffer window
type window struct {
	elements []*Message
	size     int
}

// sliding window for i slots
func (w *window) sliding(i int) {
	newSlice := make([]*Message, i)
	if i >= w.size {
		w.elements = newSlice
	} else {
		w.elements = append(w.elements[i:], newSlice...)
	}
}

func (w *window) slidingAndAddBack(i int, msg *Message) {
	w.sliding(i)
	w.elements[w.size-1] = msg
}

func (w *window) replace(i int, msg *Message) {
	w.elements[i] = msg
}

func NewWindow(size int) *window {
	w := &window{
		elements: make([]*Message, size),
		size:     size,
	}
	return w
}
