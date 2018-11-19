build: clean
	-mkdir ./bin
	GOOS=linux GOARCH=amd64 go build -a -o bin/pgpq cmd/pgpq/pgpq.go

clean:
	-rm -rf ./bin
