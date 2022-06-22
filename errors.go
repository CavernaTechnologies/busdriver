package busdriver

type RunningErr struct {
	info string
}

func (e *RunningErr) Error() string {
	return e.info
}

type NilReceiverErr struct {
	info string
}

func (e *NilReceiverErr) Error() string {
	return e.info
}

type FinishedErr struct {
	info string
}

func (e *FinishedErr) Error() string {
	return e.info
}

type ReceiveErr struct {
	info string
}

func (e *ReceiveErr) Error() string {
	return e.info
}