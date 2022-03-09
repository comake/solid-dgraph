# Dgraph database backend for the Community Solid Server

Store data in your Solid Pod with a [Dgraph Database](https://dgraph.io/).


## How to install
From the npm package registry:
```shell
mkdir my-server
cd my-server
npm install @solid/community-server@v0.5.0 @comake/solid-dgraph
```

As a developer:
```shell
git clone https://github.com/comake/solid-dgraph.git
cd solid-hue
npm ci
```

## How to configure
TODO

## How to run
Execute the following command:
```shell
npx community-solid-server -c settings.json -m .
```

Now you can access your lights
from http://localhost:3000/home/lights


## License

©2022-present Comake, Inc., [MIT License](https://github.com/RubenVerborgh/philips-hue/blob/master/LICENSE.md)
