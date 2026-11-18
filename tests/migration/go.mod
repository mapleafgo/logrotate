module example.com/logrotate-migration-test

go 1.26.0

require (
	github.com/mapleafgo/logrotate v0.1.0
	github.com/mapleafgo/logrotate/v2 v2.0.0
)

replace github.com/mapleafgo/logrotate/v2 v2.0.0 => ../..
