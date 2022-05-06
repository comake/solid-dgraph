import * as grpc from '@grpc/grpc-js';
import dgraph, { Operation } from 'dgraph-js';
import { MAX_TRANSACTION_RETRIES } from './DgraphUtil';
import type { DgraphConfiguration } from './DgraphUtil';

export class DgraphClient {
  private readonly dgraphClient: dgraph.DgraphClient;

  public constructor(configuration: DgraphConfiguration) {
    const dgraphAddress = `${configuration.connectionUri}:${configuration.grpcPort}`;
    const dgraphClientSub = new dgraph.DgraphClientStub(
      dgraphAddress,
      grpc.credentials.createInsecure(),
    );
    this.dgraphClient = new dgraph.DgraphClient(dgraphClientSub);
  }

  public async setSchema(schema: string): Promise<void> {
    const operation = new Operation();
    operation.setSchema(schema);
    await this.dgraphClient.alter(operation);
  }

  public async sendDgraphUpsert(queries: string[], delNquads: string[], setNquads: string[]): Promise<void> {
    const mutation = this.createMutation(delNquads, setNquads);
    const request = new dgraph.Request();
    const query = `query { ${queries.join('\n')} }`;
    request.setQuery(query);
    request.setMutationsList([ mutation ]);
    request.setCommitNow(true);
    await this.performBlockWithTransaction(
      async(transaction: dgraph.Txn): Promise<void> => {
        await transaction.doRequest(request);
      },
    );
  }

  public async sendDgraphQuery(query: string, vars?: Record<string, any>): Promise<any> {
    return this.performBlockWithTransaction(
      async(transaction: dgraph.Txn): Promise<any> => {
        const response = await transaction.queryWithVars(query, vars);
        return response.getJson();
      },
    );
  }

  private async performBlockWithTransaction<T>(
    transactionBlock: (transaction: dgraph.Txn) => Promise<T>,
    tries = 1,
  ): Promise<T> {
    const transaction = this.dgraphClient.newTxn();
    try {
      return await transactionBlock(transaction);
    } catch (error: unknown) {
      if (this.isRetriableTransactionError(error) && tries < MAX_TRANSACTION_RETRIES) {
        return await this.performBlockWithTransaction(transactionBlock, tries + 1);
      }
      throw error;
    } finally {
      await transaction.discard();
    }
  }

  private createMutation(delNquads: string[], setNquads: string[]): dgraph.Mutation {
    const mutation = new dgraph.Mutation();
    if (setNquads.length > 0) {
      mutation.setSetNquads(setNquads.join('\n'));
    }
    if (delNquads.length > 0) {
      mutation.setDelNquads(delNquads.join('\n'));
    }
    return mutation;
  }

  private isRetriableTransactionError(error: unknown): boolean {
    return error === dgraph.ERR_ABORTED ||
      error === '14 UNAVAILABLE: Connection dropped' ||
      error === '14 UNAVAILABLE: read ECONNRESET';
  }
}
