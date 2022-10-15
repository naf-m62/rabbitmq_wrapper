package wrapper

type Middlewares struct {
	MidList []func(m *Middlewares) error
	h       func() error
	current int
	len     int
}

func (m *Middlewares) Next() error {
	if m.current < m.len {
		m.current++
		return m.MidList[m.current-1](m)
	}
	return m.h()
}
