# Dgraph database backend extension for the Community Solid Server

Store data in your Solid Pod with a [Dgraph Database](https://dgraph.io/).

### Disclaimer

The schema and queries within this repository are built with the assumption that your Dgraph instance uses a custom fork of Dgraph where the predicate list feature has been re-added. This is so that we can query for any arbitrary/dynamic predicates a node may have. [unigraph-dev/dgraph](https://github.com/unigraph-dev/dgraph) is an example where `expand(_userpredicate_)` can be used to fetch all the predicates a node has. It was used to develop this repo.


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
Create a `config.json` file from [this template](https://github.com/comake/solid-dgraph/blob/main/config-example.json), and fill out your settings. The only important change to make to use this repo and thus dgraph as your backend is changing the line which uses `files-scs:config/storage/backend/*.json` to  `files-csd:config/dgraph.json`.

## How to run
Execute the following command:
```shell
npx community-solid-server -c config.json -m .
```

## TODO
- [ ] Add option to create secure connection to Dgraph (i.e. instead of `grpc.credentials.createInsecure()`)


## License

©2022-present Comake, Inc., [MIT License](https://github.com/comake/solid-dgraph/blob/main/LICENSE)
