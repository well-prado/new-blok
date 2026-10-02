package order

type Address struct {
	City string
}

type Order struct {
	ID      string
	Address *Address
	Items   []string
}
