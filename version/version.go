package version

import (
	"fmt"
	"html/template"
	"os"
	"runtime"
)

const (
	templateVersion = `
Version: {{.Version}}
Commit: {{.Commit}}
Branch: {{.Branch}}
OS/Arch: {{.Os}}/{{.Arch}}
Go version: {{.GoVersion}}`
)

func Info(version, commit, branch string) error {
	v := struct {
		Version   string
		Commit    string
		Branch    string
		Os        string
		Arch      string
		GoVersion string
	}{
		Version:   version,
		Commit:    commit,
		Branch:    branch,
		Os:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		GoVersion: runtime.Version(),
	}
	if e := printVersion(v); e != nil {
		return e
	}
	fmt.Print("\n")
	return nil
}

func printVersion(v interface{}) error {
	tmpl, e := template.New("").Parse(templateVersion)
	if e != nil {
		return e
	}
	return tmpl.Execute(os.Stdout, v)
}
