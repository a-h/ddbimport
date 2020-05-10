FROM alpine:latest  

RUN apk --no-cache add ca-certificates

COPY ./ddbimport_linux ddbimport

CMD ["./ddbimport"]
