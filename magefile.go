//+build mage

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/shabbyrobe/gocovmerge"
	"golang.org/x/tools/cover"
)

func Cover() {
	pkgs := goList()

	var files []string

	for idx, pkg := range pkgs {
		covFile := fmt.Sprintf("cover-%d.out", idx)
		files = append(files, covFile)
		cmd := exec.Command("go", "test",
			fmt.Sprintf("-coverprofile=%s", covFile),
			fmt.Sprintf("-coverpkg=%s", strings.Join(pkgs, ",")),
			pkg,
		)
		if err := cmd.Run(); err != nil {
			panic(err)
		}
	}

	var merged []*cover.Profile
	for _, file := range files {
		profiles, err := cover.ParseProfiles(file)
		if err != nil {
			panic(fmt.Errorf("failed to parse profiles: %v", err))
		}
		for _, p := range profiles {
			merged = gocovmerge.AddProfile(merged, p)
		}
		os.Remove(file)
	}

	gocovmerge.DumpProfiles(merged, os.Stdout)
}

func goList() (pkgs []string) {
	cmd := exec.Command("go", "list", "./...")

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}
		pkgs = append(pkgs, line)
	}
	return pkgs
}
