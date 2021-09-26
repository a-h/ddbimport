FROM alpine:latest  

RUN apk --no-cache add ca-certificates

COPY ./dist/ddbimport_linux_amd64 /ddbimport

CMD ["./ddbimport"]
