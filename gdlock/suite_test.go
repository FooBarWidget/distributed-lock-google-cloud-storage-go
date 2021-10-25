package gdlock

import (
	"errors"
	"os"
	"testing"

	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	_, err := os.Stat("../.env")
	if err == nil {
		err = godotenv.Load("../.env")
		if err != nil {
			panic("Error loading .env: " + err.Error())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		panic("Error checking whether .env exists: " + err.Error())
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Gdlock Suite")
}
