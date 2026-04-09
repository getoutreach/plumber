package templates

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/getoutreach/plumber/internal/command/shape/contract"
	"github.com/samber/lo"
)

func checkoutGit(cfg *contract.PlumberTemplateGitSourceConfig, cacheDir string) error {
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}
	repoPath := gitRepoPath(cacheDir, cfg.Repository, cfg.Ref)

	err := os.MkdirAll(repoPath, os.ModePerm)
	if err != nil {
		return err
	}

	isGitRepo, err := exists(path.Join(repoPath, ".git"))
	if err != nil {
		return err
	}

	// init repo if it doesn't exist
	if !isGitRepo {
		cmd := execCommand("git", "clone", "--no-checkout", "--depth=1", "--filter=tree:0", "--branch="+cfg.Ref, cfg.Repository, repoPath)
		stdout, err := cmd.Output()
		if err != nil {
			return err
		}
		fmt.Println(string(stdout))
	}

	err = os.Chdir(repoPath)
	if err != nil {
		return err
	}

	dirs := lo.Map(cfg.Templates, func(t contract.PlumberTemplateConfig, _ int) string {
		p := path.Dir(t.Path)
		p = strings.TrimPrefix(p, "./")
		p = strings.TrimPrefix(p, "/")
		return "/" + p
	})

	cmd := execCommand("git", append([]string{"sparse-checkout", "set", "--no-cone"}, dirs...)...)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}
	fmt.Println(string(stdout))

	cmd = execCommand("git", "checkout", "origin/"+cfg.Ref)
	stdout, err = cmd.Output()
	if err != nil {
		return err
	}
	fmt.Println(string(stdout))

	return nil
}

func gitRepoPath(cacheDir, repository, ref string) string {
	if ref == "" {
		ref = "main"
	}
	repository = strings.TrimPrefix(repository, "https://")
	repository = strings.TrimPrefix(repository, "git@")
	repository = strings.ReplaceAll(repository, "/", "-")
	return path.Join(cacheDir, repository, ref)
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func execCommand(name string, args ...string) *exec.Cmd {
	fmt.Println(name, strings.Join(args, " "))
	return exec.Command(name, args...)
}
