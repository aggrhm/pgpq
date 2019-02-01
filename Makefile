build: clean
	-mkdir ./bin
	GOOS=linux GOARCH=amd64 go build -o bin/pgpq cmd/pgpq/pgpq.go

clean:
	-rm -rf ./bin ./dist

dist: build
	-mkdir ./dist
	tar -zcvf ./dist/pgpq.tgz --xform="s,^\.,pgpq," ./bin ./db

release: dist
	github-release release --user agquick --repo pgpq --tag $(tag)
	github-release upload --user agquick --repo pgpq --tag $(tag) --name pgpq.tgz --file ./dist/pgpq.tgz
