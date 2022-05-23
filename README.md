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
npm install @solid/community-server@v4.0.0 @comake/solid-dgraph
```

#### Configure
In your `my-server` folder (or whatever the name of your project is):

Create a `config.json` file from [this template](https://github.com/comake/solid-dgraph/blob/main/config-example.json), and fill out your settings. The only important changes to make to the [default config](https://github.com/CommunitySolidServer/CommunitySolidServer/blob/main/config/default.json) are to add `solid-dgraph` into `@context` and change the backend to dgraph:

```diff
-"files-scs:config/storage/backend/*.json",
+"files-csd:config/storage/backend/dgraph.json",
```


Optionally, you can change the connection settings for your Dgraph database by adding parameters to the `DgraphDataAccessor` in the `@graph` section such as:
```json
{
  "@id": "urn:solid-dgraph:default:DgraphDataAccessor",
  "comment": "The configuration to connect to the Dgraph instance.",
  "DgraphDataAccessor:_configuration_connectionUri": "https://mySpecialConnectionUri",
  "DgraphDataAccessor:_configuration_grpcPort": "1234",
}
```
If you do not include this section, `connectionUri` defaults to `localhost` and `grpcPort` defaults to `9080`, which are the Dgraph defaults.

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
- [ ] Add separate unit tests for DgraphClient.ts and mock it in the tests for DgraphDataAccessor
- [ ] Add option to create secure connection to Dgraph (i.e. instead of `grpc.credentials.createInsecure()`)
- [ ] Integration tests

## License

©2022-present Comake, Inc., [MIT License](https://github.com/comake/solid-dgraph/blob/main/LICENSE)
