package ui

import (
	_ "embed"
	"io"
	"text/template"
)

var (
	//go:embed layout.html
	layoutHTML string
	//go:embed storacha-logo.svg
	logoSVG string
	//go:embed pending-page.html
	pendingPageHTML string
	//go:embed validate-page.html
	validatePageHTML string
	//go:embed error-page.html
	errorPageHTML string
)

var (
	layoutTpl       = template.Must(template.New("layout").Parse(layoutHTML))
	pendingPageTpl  = template.Must(template.Must(layoutTpl.Clone()).Parse(pendingPageHTML))
	validatePageTpl = template.Must(template.Must(layoutTpl.Clone()).Parse(validatePageHTML))
	errorPageTpl    = template.Must(template.Must(layoutTpl.Clone()).Parse(errorPageHTML))
)

type pendingPageData struct {
	Logo        string
	AutoApprove bool
}

type validatePageData struct {
	Logo     string
	Email    string
	Audience string
	UCAN     string
}

type errorPageData struct {
	Logo    string
	Message string
}

func PendingValidateEmailPage(autoApprove bool) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		err := pendingPageTpl.Execute(w, pendingPageData{
			Logo:        logoSVG,
			AutoApprove: autoApprove,
		})
		w.CloseWithError(err)
	}()
	return r, nil
}

func ValidateEmailPage(ucan string, email string, audience string) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		err := validatePageTpl.Execute(w, validatePageData{
			Logo:     logoSVG,
			UCAN:     ucan,
			Email:    email,
			Audience: audience,
		})
		w.CloseWithError(err)
	}()
	return r, nil
}

func ErrorPage(message string) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		err := errorPageTpl.Execute(w, errorPageData{
			Logo:    logoSVG,
			Message: message,
		})
		w.CloseWithError(err)
	}()
	return r, nil
}
