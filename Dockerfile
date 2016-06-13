FROM alpine:3.3

ADD *.go /concept-publisher/

RUN apk add --update bash \
  && apk --update add git bzr \
  && apk --update add go \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/concept-publisher" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && mv concept-publisher/* $GOPATH/src/${REPO_PATH} \
  && rm -rf concept-publisher \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -t ./... \
  && go build \
  && mv concept-publisher /concept-publisher \
  && apk del go git bzr \
  && rm -rf $GOPATH /var/cache/apk/*

CMD [ "/concept-publisher" ]