FROM alpine:latest  

RUN apk --no-cache add ca-certificates

COPY ddbimport /ddbimport

CMD ["./ddbimport"]
