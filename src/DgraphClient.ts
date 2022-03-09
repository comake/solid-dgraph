import type { DgraphClient as ActualDgraphClient } from 'dgraph-js';

export class DgraphClient {
  private static readonly instance = new DgraphClient();

  private dgraphClient: ActualDgraphClient | undefined;

  private constructor() {
    // Singleton instance
  }

  public static getInstance(): DgraphClient {
    return DgraphClient.instance;
  }

  public resetDgraphClient(): void {
    this.dgraphClient = undefined;
  }

  public get client(): ActualDgraphClient {
    if (!this.dgraphClient) {
      throw new Error('No dgraph client has been initialized.');
    }
    return this.dgraphClient;
  }

  public set client(loggerFactory: ActualDgraphClient) {
    this.dgraphClient = loggerFactory;
  }
}
