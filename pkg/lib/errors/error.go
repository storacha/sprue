package errors

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
)

type ErrorModel struct {
	ErrorName string
	Message   string
}

// New creates an IPLD error that has a name as well as a message.
func New(name, message string, args ...any) ErrorModel {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	return ErrorModel{
		ErrorName: name,
		Message:   message,
	}
}

func (em ErrorModel) Name() string {
	return em.ErrorName
}

func (em ErrorModel) Error() string {
	return em.Message
}

func (em ErrorModel) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&em, ErrorType(), types.Converters...)
}
