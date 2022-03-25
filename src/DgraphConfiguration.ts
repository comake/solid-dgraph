export interface DgraphConfigurationArgs {
  connectionUri: string;
  grpcPort: string;
  schema?: string;
}

export class DgraphConfiguration {
  public connectionUri: string;
  public grpcPort: string;
  public schema?: string;

  public constructor(args: DgraphConfigurationArgs) {
    this.connectionUri = args.connectionUri;
    this.grpcPort = args.grpcPort;
    this.schema = args.schema;
  }
}
