import { DgraphClient } from '../../src/DgraphClient';

describe('DgraphClient', (): void => {
  let dummyDgraphClient: any;
  beforeEach(async(): Promise<void> => {
    DgraphClient.getInstance().resetDgraphClient();
    dummyDgraphClient = {};
  });

  it('is a singleton.', async(): Promise<void> => {
    expect(DgraphClient.getInstance()).toBeInstanceOf(DgraphClient);
  });

  it('throws when retrieving the inner client if none has been set.', async(): Promise<void> => {
    expect((): any => DgraphClient.getInstance().client)
      .toThrow('No dgraph client has been initialized.');
  });

  it('Returns the inner client if one has been set.', async(): Promise<void> => {
    DgraphClient.getInstance().client = dummyDgraphClient;
    expect(DgraphClient.getInstance().client).toBe(dummyDgraphClient);
  });
});
