package agent

import (
	"iter"

	"github.com/alanshaw/ucantone/ucan"
)

func Index(message ucan.Container) iter.Seq2[IndexEntry, error] {
	return func(yield func(IndexEntry, error) bool) {
		for _, inv := range message.Invocations() {
			entry := IndexEntry{
				Invocation: &InvocationSource{
					Task:       inv.Task().Link(),
					Invocation: inv,
				},
			}
			if !yield(entry, nil) {
				return
			}
		}
		for _, rcpt := range message.Receipts() {
			entry := IndexEntry{
				Receipt: &ReceiptSource{
					Task:    rcpt.Ran(),
					Receipt: rcpt,
				},
			}
			if !yield(entry, nil) {
				return
			}
		}
	}
}
