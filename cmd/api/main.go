package main

import (
	"go.uber.org/fx"

	fxmodule "github.com/yourusername/hexagon-app/fx"
)

func main() {
	fx.New(
		fxmodule.Module,
	).Run()
}
