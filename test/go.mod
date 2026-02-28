module dragonfly5/runtest

go 1.24.0

require golang.org/x/sync v0.19.0

require github.com/matoous/go-nanoid/v2 v2.1.0 // indirect

require (
	github.com/cycau/dragonfly5_client_go v0.0.0-20260226083650-c40c8d50b37e
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cycau/dragonfly5_client_go => ../
