import { promises as fsPromises } from 'fs';
import * as grpc from '@grpc/grpc-js';
import dgraph, { Operation } from 'dgraph-js';
import type { DgraphClient } from 'dgraph-js';
import { Initializer, getLoggerFor } from '@solid/community-server';
import { DEFAULT_SCHEMA, setGlobalDgraphClientInstance } from './DgraphUtil';

export interface DgraphConfiguration {
  connectionUri: string;
  ports: { grpc: string; zero: string };
  schema?: string;
}

export class DgraphClientInitializer extends Initializer {
  protected readonly logger = getLoggerFor(this);

  private readonly configFilePath: string;

  public constructor(configFilePath: string) {
    super();
    this.configFilePath = configFilePath;
  }

  public async handle(): Promise<void> {
    const configText = await fsPromises.readFile(this.configFilePath, 'utf8');
    const configuration: DgraphConfiguration = JSON.parse(configText);
    const dgraphClient = await this.createDgraphClientFromConfiguration(configuration);
    await this.setDgraphSchema(dgraphClient, configuration.schema ?? DEFAULT_SCHEMA);
    setGlobalDgraphClientInstance(dgraphClient);
    this.logger.info(`Initialized Dgraph Client`);
  }

  private async createDgraphClientFromConfiguration(config: DgraphConfiguration): Promise<DgraphClient> {
    const dgraphClientSub = new dgraph.DgraphClientStub(
      `${config.connectionUri}:${config.ports.grpc}`,
      grpc.credentials.createInsecure(),
    );
    return new dgraph.DgraphClient(dgraphClientSub);
  }

  private async setDgraphSchema(dgraphClient: DgraphClient, schema: string): Promise<void> {
    const operation = new Operation();
    operation.setSchema(schema);
    await dgraphClient.alter(operation);
  }
}
