VERSION := $(shell cat VERSION)

build:
	docker build --build-arg METABASE_VERSION=$(VERSION) -t metabase:$(VERSION)-databricks -f Dockerfile .

release:
	docker tag metabase:$(VERSION)-databricks alexviala/metabase:$(VERSION)-databricks
	docker push alexviala/metabase:$(VERSION)-databricks
