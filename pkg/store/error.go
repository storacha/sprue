package store

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-capabilities/pkg/types"
	"github.com/storacha/go-ucanto/core/ipld"
)

type ErrorModel struct {
	ErrorName string
	Message   string
}

// NewError creates an error that has a name as well as a message.
func NewError(name, message string) ErrorModel {
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
