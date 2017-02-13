FROM alpine:3.4

ADD *.go /concept-publisher/

RUN apk --no-cache --virtual .build-dependencies add git bzr go \
  && apk --no-cache --upgrade add ca-certificates bash \
  && update-ca-certificates --fresh \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/concept-publisher" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && mv concept-publisher/* $GOPATH/src/${REPO_PATH} \
  && rm -rf concept-publisher \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -t ./... \
  && go build -v \
  && mv concept-publisher /concept-publisher \
  && apk del .build-dependencies \
  && rm -rf $GOPATH

EXPOSE 8080

CMD exec /concept-publisher