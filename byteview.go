package dcache

type ByteView struct {
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

func (b ByteView) String() string {
	return string(b.b)
}

func (b ByteView) Clone() []byte {
	return CloneBytes(b.b)
}

func CloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
