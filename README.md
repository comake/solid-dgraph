# Dgraph database backend extension for the Community Solid Server

Store data in your Solid Pod with a [Dgraph Database](https://dgraph.io/).

### Disclaimer

The schema and queries within this repository are built with the assumption that your Dgraph instance uses a custom fork of Dgraph where the predicate list feature has been re-added. This is so that we can query for any arbitrary/dynamic predicates a node may have. [unigraph-dev/dgraph](https://github.com/unigraph-dev/dgraph) is an example where `expand(_userpredicate_)` can be used to fetch all the predicates a node has. It was used to develop this repo.


## How to use Solid Dgraph

#### Install
From the npm package registry:
```shell
mkdir my-server
cd my-server
npm install @solid/community-server@v3.0.0 @comake/solid-dgraph
```

#### Configure
In your `my-server` folder (or whatever the name of your project is):
1. Create a `config.json` file from [this template](https://github.com/comake/solid-dgraph/blob/main/config-example.json), and fill out your settings. The only important change to make to the [default config from Community Solid Server](https://github.com/CommunitySolidServer/CommunitySolidServer/blob/main/config/default.json) is to change the line which uses `files-scs:config/storage/backend/*.json` to  `files-csd:config/dgraph.json`.
2. Create a `dgraph.json` file inside the `config` folder of your project from [this template](https://github.com/comake/solid-dgraph/blob/main/dgraph-config-example.json), and fill out your settings. This file should be a JSON object with keys `connectionUri` and `ports`. It may optionally have a `schema` key if you'd like to override the default schema found [here](https://github.com/comake/solid-dgraph/blob/main/src/DgraphUtil.ts#L5).

#### Run
Execute the following command:
```shell
npx community-solid-server -c config.json -m .
```
Or add the command to `scripts` inside your package.json like so:
```json
{
  "scripts": {
    "start": "npx community-solid-server -c config.json -m ."
  }
}
```

## How to contribute to Solid Dgraph

Execute the following commands:
```shell
git clone https://github.com/comake/solid-dgraph.git
cd solid-dgraph
npm ci
```

## TODO
- [ ] Add option to create secure connection to Dgraph (i.e. instead of `grpc.credentials.createInsecure()`)

## License

©2022-present Comake, Inc., [MIT License](https://github.com/comake/solid-dgraph/blob/main/LICENSE)
