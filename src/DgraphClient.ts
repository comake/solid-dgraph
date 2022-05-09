import * as grpc from '@grpc/grpc-js';
import dgraph, { Operation } from 'dgraph-js';
import { MAX_TRANSACTION_RETRIES } from './DgraphUtil';
import type { DgraphConfiguration } from './DgraphUtil';

const GRPC_UNAVAILABLE_ERROR_CODE = 14;
const GRPC_UNAVAILABLE_ERROR_DETAILS = new Set([
  'Connection dropped',
  'read ECONNRESET',
  'No connection established',
]);

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
      if (error && this.isRetriableTransactionError(error) && tries < MAX_TRANSACTION_RETRIES) {
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
    return error !== null && (
      error === dgraph.ERR_ABORTED ||
      this.isRetriableGrpcError(error as grpc.ServiceError)
    );
  }

  private isRetriableGrpcError(error: grpc.ServiceError): boolean {
    return error.code === GRPC_UNAVAILABLE_ERROR_CODE &&
      GRPC_UNAVAILABLE_ERROR_DETAILS.has(error.details);
  }
}
