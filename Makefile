test:
	@ go test ./... -v

bench:
	@go test ./... -bench=. -run=^# -count=10 -v

.PHONY: bench test
