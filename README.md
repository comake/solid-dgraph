# Dgraph database backend for the Community Solid Server

Store data in your Solid Pod with a [Dgraph Database](https://dgraph.io/).

### Disclaimer

The schema and queries within this repository are built with the assumption that your Dgraph instance uses a custom fork of Dgraph where the predicate list feature has been re-added. This is so that we can query for any arbitrary/dynamic predicate a node may have. An example can be found here: [unigraph-dev/dgraph](https://github.com/unigraph-dev/dgraph).


## How to install
From the npm package registry:
```shell
mkdir my-server
cd my-server
npm install @solid/community-server@v3.0.0 @comake/solid-dgraph
```

As a developer:
```shell
git clone https://github.com/comake/solid-dgraph.git
cd solid-dgraph
npm ci
```

## How to configure
TODO

## How to run
Execute the following command:
```shell
npx community-solid-server -c settings.json -m .
```

## TODO
- [ ] Fill out the "How to configure" section
- [ ] Add option to create secure connection to Dgraph (i.e. instead of `grpc.credentials.createInsecure()`)
- [ ] Figure out configs and how to run this


## License

©2022-present Comake, Inc., [MIT License](https://github.com/RubenVerborgh/philips-hue/blob/master/LICENSE.md)
