# We specify the base image we need for our
# go application
FROM golang:1.13-buster as build
# We create an /app directory within our
# image that will hold our application source
# files
RUN mkdir /app
# We copy everything in the root directory
# into our /app directory
ADD . /app
# We specify that we now wish to execute
# any further commands inside our /app
# directory
WORKDIR /app
# we run go build to compile the binary
# executable of our Go program
ENV GOPROXY=direct
RUN go build -o kudos-api ./pkg
# Our start command which kicks off
# our newly created binary executable

# Now copy it into our base image.
FROM gcr.io/distroless/base-debian10
COPY --from=build /app/kudos-api /

CMD ["/kudos-api"]

EXPOSE 80
