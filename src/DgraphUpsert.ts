export class DgraphUpsert {
  public readonly queries: string[] = [];
  public readonly setNquads: string[] = [];
  public readonly delNquads: string[] = [];

  public addSetNquad(nQuad: string): void {
    this.setNquads.push(nQuad);
  }

  public addDelNquad(nQuad: string): void {
    this.delNquads.push(nQuad);
  }

  public addQuery(query: string): void {
    this.queries.push(query);
  }
}
