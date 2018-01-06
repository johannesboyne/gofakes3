FROM scratch
ADD ca-certificates.crt /etc/ssl/certs/
ADD main /
EXPOSE 9000
CMD ["/main"]
