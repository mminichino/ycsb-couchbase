.PHONY:	commit build package release
export PROJECT_NAME := $$(basename $$(pwd))
export PROJECT_VERSION := $(shell cat VERSION)

commit:
		git commit -am "Version $(shell cat VERSION)"
		git push -u origin
branch:
		git checkout -b "Version_$(shell cat VERSION)"
		git push --set-upstream origin "Version_$(shell cat VERSION)"
merge:
		git checkout main
		git pull origin main
		git merge "Version_$(shell cat VERSION)"
		git push origin main
remote:
		git push cblabs main
sync:
		rsync -acv --exclude db/ --exclude .DS_Store core/src/main/java/site/ycsb/ ../YCSB/core/src/main/java/site/ycsb/
		rsync -acv --exclude db/ --exclude .DS_Store core/src/test/java/site/ycsb/ ../YCSB/core/src/test/java/site/ycsb/
		rsync -acv --exclude .DS_Store core/src/main/java/site/ycsb/db/couchbase3/ ../YCSB/couchbase3/src/main/java/site/ycsb/db/couchbase3/
		rsync -acv --exclude .DS_Store core/src/main/resources/logback.xml ../YCSB/couchbase3/src/main/resources/
		rsync -acv --exclude .DS_Store workloads/ ../YCSB/workloads/
		rsync -acv --exclude .DS_Store bin/ ../YCSB/bin/
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
		bumpversion --allow-dirty build
		mvn clean install
release: tag download remote_tag remote_download remote
download:
		$(eval REV_FILE := $(shell ls -tr core/target/ycsb-couchbase.zip | tail -1))
		gh release upload --clobber -R "mminichino/$(PROJECT_NAME)" $(PROJECT_VERSION) $(REV_FILE)
tag:
		if gh release view -R "mminichino/$(PROJECT_NAME)" $(PROJECT_VERSION) >/dev/null 2>&1 ; then gh release delete -R "mminichino/$(PROJECT_NAME)" $(PROJECT_VERSION) --cleanup-tag -y ; fi
		gh release create -R "mminichino/$(PROJECT_NAME)" \
		-t "Release $(PROJECT_VERSION)" \
		-n "Release $(PROJECT_VERSION)" \
		$(PROJECT_VERSION)
remote_download:
		$(eval REV_FILE := $(shell ls -tr core/target/ycsb-couchbase.zip | tail -1))
		gh release upload --clobber -R "couchbaselabs/$(PROJECT_NAME)" $(PROJECT_VERSION) $(REV_FILE)
remote_tag:
		if gh release view -R "couchbaselabs/$(PROJECT_NAME)" $(PROJECT_VERSION) >/dev/null 2>&1 ; then gh release delete -R "couchbaselabs/$(PROJECT_NAME)" $(PROJECT_VERSION) --cleanup-tag -y ; fi
		gh release create -R "couchbaselabs/$(PROJECT_NAME)" \
		-t "Release $(PROJECT_VERSION)" \
		-n "Release $(PROJECT_VERSION)" \
		$(PROJECT_VERSION)
