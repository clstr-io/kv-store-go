package store

// NotFoundError is returned when a key does not exist in the store.
type NotFoundError struct{}

func (e *NotFoundError) Error() string {
	return "key not found"
}
