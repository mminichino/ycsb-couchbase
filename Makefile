.PHONY:	commit build package release
export PROJECT_NAME := $$(basename $$(pwd))
export PROJECT_VERSION := $(shell cat VERSION)

commit:
		git commit -am "Version $(shell cat VERSION)"
		git push
sync:
		rsync -acv --exclude db/ --exclude .DS_Store core/src/main/java/site/ycsb/ ../YCSB/core/src/main/java/site/ycsb/
		rsync -acv --exclude db/ --exclude .DS_Store core/src/test/java/site/ycsb/ ../YCSB/core/src/test/java/site/ycsb/
		rsync -acv --exclude .DS_Store core/src/main/java/site/ycsb/db/couchbase3/ ../YCSB/couchbase3/src/main/java/site/ycsb/db/couchbase3/
		rsync -acv --exclude .DS_Store core/src/main/resources/logback.xml ../YCSB/couchbase3/src/main/resources/
		rsync -acv --exclude .DS_Store workloads/ ../YCSB/workloads/
build:
		bumpversion --allow-dirty build
patch:
		bumpversion --allow-dirty patch
minor:
		bumpversion --allow-dirty minor
major:
		bumpversion --allow-dirty major
setup:
		python setup.py sdist
package:
		mvn clean install
release: package download
download:
		gh release create -R "mminichino/$(PROJECT_NAME)" \
		-t "Release $(PROJECT_VERSION)" \
		-n "Release $(PROJECT_VERSION)" \
		$(PROJECT_VERSION) \
		./core/target/ycsb-couchbase.zip
recall:
		gh release delete $(PROJECT_VERSION) \
		--cleanup-tag -y
